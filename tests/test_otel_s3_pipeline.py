"""Tests for the S3 OTEL context propagation utilities."""
from __future__ import annotations

import os
import sys

import pytest
from unittest.mock import MagicMock, patch

# Ensure the dags directory is on the path so we can import otel_s3_pipeline
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from opentelemetry import propagate, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import opentelemetry.trace as _trace_module


def _reset_otel_global():
    """Reset OTel's set-once provider guard so tests can install fresh providers."""
    _trace_module._TRACER_PROVIDER_SET_ONCE._done = False
    _trace_module._TRACER_PROVIDER = None


@pytest.fixture(autouse=True)
def reset_otel():
    """Install a fresh in-memory TracerProvider before each test."""
    _reset_otel_global()

    # Also reset airflow_otel's module-level provider if the package is installed,
    # so _get_tracer() falls back to the global provider we control here.
    try:
        import airflow_otel._instrumentation as _ao
        _ao._tracer_provider = None
    except ImportError:
        pass

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    propagate.set_global_textmap(TraceContextTextMapPropagator())

    yield provider, exporter

    exporter.clear()
    _reset_otel_global()


# ---------------------------------------------------------------------------
# s3_put_with_context
# ---------------------------------------------------------------------------


def test_put_injects_traceparent(reset_otel):
    from otel_s3_pipeline.s3_otel import s3_put_with_context

    mock_s3 = MagicMock()
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_s3):
        s3_put_with_context("my-bucket", "prefix/obj.json", b'{"x":1}')

    call_kwargs = mock_s3.put_object.call_args[1]
    assert "traceparent" in call_kwargs["Metadata"], "traceparent must be in S3 metadata"
    assert call_kwargs["Metadata"]["traceparent"].startswith("00-")


def test_put_emits_producer_span(reset_otel):
    _, exporter = reset_otel
    from otel_s3_pipeline.s3_otel import s3_put_with_context

    mock_s3 = MagicMock()
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_s3):
        s3_put_with_context("my-bucket", "prefix/obj.json", b"data")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "s3.put_object"
    assert span.kind == trace.SpanKind.PRODUCER
    assert span.attributes["messaging.system"] == "aws_s3"
    assert span.attributes["messaging.destination.name"] == "my-bucket"
    assert span.attributes["messaging.s3.key"] == "prefix/obj.json"


def test_put_passes_extra_kwargs(reset_otel):
    from otel_s3_pipeline.s3_otel import s3_put_with_context

    mock_s3 = MagicMock()
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_s3):
        s3_put_with_context(
            "my-bucket", "obj.json", b"data", ContentType="application/json"
        )

    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["ContentType"] == "application/json"


# ---------------------------------------------------------------------------
# s3_get_with_link
# ---------------------------------------------------------------------------


def _make_mock_s3_response(metadata: dict, body: bytes) -> MagicMock:
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Metadata": metadata,
        "Body": MagicMock(read=MagicMock(return_value=body)),
    }
    return mock_s3


def test_get_creates_consumer_span_as_child(reset_otel):
    """Consumer span is a child of the producer span — same trace ID."""
    _, exporter = reset_otel
    from otel_s3_pipeline.s3_otel import s3_get_with_context

    carrier: dict[str, str] = {}
    with trace.get_tracer("upstream").start_as_current_span("upstream.task") as span:
        propagate.inject(carrier)
        upstream_trace_id = span.get_span_context().trace_id
    exporter.clear()

    mock_s3 = _make_mock_s3_response(carrier, b"payload")
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_s3):
        result = s3_get_with_context("my-bucket", "prefix/obj.json")

    assert result == b"payload"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "s3.get_object"
    assert span.kind == trace.SpanKind.CONSUMER
    assert span.attributes["messaging.system"] == "aws_s3"
    assert span.attributes["messaging.source.name"] == "my-bucket"

    # Parent-child: consumer shares the producer's trace ID
    assert span.context.trace_id == upstream_trace_id, \
        "consumer span must be in the same trace as the producer"
    assert span.links == (), "no span links — the connection is via parent context"


def test_get_new_root_when_metadata_absent(reset_otel):
    """Objects with no traceparent produce a new root span, not a crash."""
    _, exporter = reset_otel
    from otel_s3_pipeline.s3_otel import s3_get_with_context

    mock_s3 = _make_mock_s3_response({}, b"legacy data")
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_s3):
        result = s3_get_with_context("my-bucket", "old-key.json")

    assert result == b"legacy data"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].parent is None  # new root span — no parent


def test_get_trace_id_matches_put(reset_otel):
    """Round-trip: consumer span shares the producer's trace ID."""
    _, exporter = reset_otel
    from otel_s3_pipeline.s3_otel import s3_put_with_context, s3_get_with_context

    written_carrier: dict[str, str] = {}
    mock_put_s3 = MagicMock()
    mock_put_s3.put_object.side_effect = lambda **kwargs: written_carrier.update(
        kwargs.get("Metadata", {})
    )

    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_put_s3):
        s3_put_with_context("bucket", "key.json", b"data")

    producer_span = exporter.get_finished_spans()[0]
    producer_trace_id = producer_span.context.trace_id
    exporter.clear()

    mock_get_s3 = _make_mock_s3_response(written_carrier, b"data")
    with patch("otel_s3_pipeline.s3_otel._s3_client", return_value=mock_get_s3):
        s3_get_with_context("bucket", "key.json")

    consumer_span = exporter.get_finished_spans()[0]
    assert consumer_span.context.trace_id == producer_trace_id
    assert consumer_span.links == ()
