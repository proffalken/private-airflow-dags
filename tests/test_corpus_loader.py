"""Tests for the CORPUS reference data loader DAG helpers."""
from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

# Stub Airflow before import
for _mod in [
    "airflow", "airflow.sdk",
    "airflow.models", "airflow.models.variable",
    "airflow.providers", "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
    "pendulum",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

import corpus_loader as cl  # noqa: E402


# ---------------------------------------------------------------------------
# _parse_corpus
# ---------------------------------------------------------------------------

SAMPLE_DATA = {
    "TIPLOCDATA": [
        {
            "STANOX": "73300",
            "TIPLOC": "SHEFLD",
            "STANME": "SHEFFIELD",
            "CRS": "SHF",
            "NLC": "7330",
            "NLCDESC": "SHEFFIELD",
        },
        {
            "STANOX": "73530",
            "TIPLOC": "DORETOT",
            "STANME": "DORE & TOTLEY",
            "CRS": "DOR",
            "NLC": "7353",
            "NLCDESC": "DORE AND TOTLEY",
        },
        # Entry with no STANOX should be filtered out
        {
            "STANOX": "",
            "TIPLOC": "NOSTAND",
            "STANME": "NO STANOX",
            "CRS": "",
            "NLC": "",
            "NLCDESC": "",
        },
        # Entry with all-zero STANOX should be filtered out
        {
            "STANOX": "00000",
            "TIPLOC": "ZEROS",
            "STANME": "ZEROS",
            "CRS": "",
            "NLC": "",
            "NLCDESC": "",
        },
        # Entry with short STANOX — should be zero-padded
        {
            "STANOX": "5005",
            "TIPLOC": "ASHCHRD",
            "STANME": "ASHCHURCH",
            "CRS": "ACH",
            "NLC": "5005",
            "NLCDESC": "ASHCHURCH FOR TEWKESBURY",
        },
    ]
}


class TestParseCorpus:
    def test_returns_list_of_tuples(self):
        records = cl._parse_corpus(SAMPLE_DATA)
        assert isinstance(records, list)
        assert all(isinstance(r, tuple) for r in records)

    def test_filters_empty_stanox(self):
        records = cl._parse_corpus(SAMPLE_DATA)
        stanoxes = [r[0] for r in records]
        assert "" not in stanoxes

    def test_filters_all_zero_stanox(self):
        records = cl._parse_corpus(SAMPLE_DATA)
        stanoxes = [r[0] for r in records]
        assert "00000" not in stanoxes

    def test_zero_pads_short_stanox(self):
        records = cl._parse_corpus(SAMPLE_DATA)
        stanoxes = [r[0] for r in records]
        assert "05005" in stanoxes

    def test_preserves_full_length_stanox(self):
        records = cl._parse_corpus(SAMPLE_DATA)
        stanoxes = [r[0] for r in records]
        assert "73300" in stanoxes

    def test_correct_record_count(self):
        # 5 entries, 2 filtered (empty + all-zeros) → 3 valid
        records = cl._parse_corpus(SAMPLE_DATA)
        assert len(records) == 3

    def test_tuple_structure(self):
        # (stanox, tiploc, stanme, crs, nlc, description)
        records = cl._parse_corpus(SAMPLE_DATA)
        shef = next(r for r in records if r[0] == "73300")
        assert shef[1] == "SHEFLD"      # tiploc
        assert shef[2] == "SHEFFIELD"   # stanme
        assert shef[3] == "SHF"         # crs
        assert shef[4] == "7330"        # nlc
        assert shef[5] == "SHEFFIELD"   # description

    def test_empty_strings_become_none(self):
        data = {"TIPLOCDATA": [
            {"STANOX": "99999", "TIPLOC": "", "STANME": "TEST", "CRS": "", "NLC": "", "NLCDESC": ""}
        ]}
        records = cl._parse_corpus(data)
        assert records[0][1] is None   # tiploc → None
        assert records[0][3] is None   # crs → None

    def test_empty_tiplocdata_returns_empty_list(self):
        assert cl._parse_corpus({"TIPLOCDATA": []}) == []

    def test_missing_tiplocdata_key_returns_empty_list(self):
        assert cl._parse_corpus({}) == []

    def test_strips_whitespace_from_values(self):
        data = {"TIPLOCDATA": [
            {"STANOX": " 73300 ", "TIPLOC": " SHEFLD ", "STANME": " SHEFFIELD ", "CRS": " SHF ", "NLC": "7330", "NLCDESC": "SHEFFIELD"}
        ]}
        records = cl._parse_corpus(data)
        assert records[0][0] == "73300"  # stripped and already 5 chars
        assert records[0][2] == "SHEFFIELD"  # stripped
