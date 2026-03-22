# Rail Network Analysis DAG

A daily Airflow pipeline that downloads UK train timetable data from [Network Rail Open Data (NROD)](https://wiki.openraildata.com/index.php/Main_Page), processes it into structured metrics, and produces a plain-English summary using a local LLM. It is instrumented end-to-end with OpenTelemetry using the `airflow-otel` library.

## What it does

Each day the pipeline:

1. Downloads two feeds from NROD in parallel:
   - **CORPUS** — a reference file mapping internal location codes (TIPLOCs) to station names and CRS codes
   - **CIF full timetable** — the complete UK train schedule (~130MB compressed, JsonTimetableV1 format)
2. Parses both feeds and loads them into PostgreSQL
3. Analyses the schedule from three independent angles (run in parallel):
   - **By operator (TOC)** — services, routes, and stations per train company
   - **By route** — frequency and operator count for every origin→destination pair
   - **By station** — total calls, operator diversity, and terminus counts per station
4. Aggregates the results into a single run summary
5. Stores the summary to the database
6. Asks a local LLM to write a plain-English narrative of the day's network

## Data flow

```
fetch_corpus ──► parse_corpus ──────────────────────────┐
                                                         ├──► analyse_by_route ──────┐
fetch_schedule ──► parse_schedule ──────────────────────┤                           │
                        │               analyse_by_station ────────────────────────► aggregate ──► store_results ──► llm_summary
                        └──► extract_toc_list ──► analyse_by_toc ───────────────────┘
```

The parallel structure is intentional: fetch and parse tasks overlap where possible, and the three analysis tasks run simultaneously since they only read from the database. This keeps total wall-clock runtime well under the sum of individual task times.

## Prerequisites

### Airflow Variables

Set these in Admin → Variables:

| Variable | Value |
|---|---|
| `NROD_USERNAME` | Your Network Rail Open Data portal email |
| `NROD_PASSWORD` | Your NROD password |

You can register for a free account at [datafeeds.networkrail.co.uk](https://datafeeds.networkrail.co.uk).

### Airflow Connection

Create a connection in Admin → Connections:

| Field | Value |
|---|---|
| Connection ID | `rail_network_db` |
| Connection Type | `Postgres` |
| Host | `ossway-pg-rw.default.svc.cluster.local` |
| Port | `5432` |
| Schema | `rail_network` |
| Login | `app` |
| Password | (the app user's database password) |

### Database

The `rail_network` database must exist on the CNPG cluster before the first run. Apply the manifest:

```bash
kubectl apply -f k8s/rail-network-db.yaml
```

The schema is created automatically by the DAG on first run — no manual migration is needed.

## Database schema

| Table | Description |
|---|---|
| `rail_locations` | TIPLOC → CRS code + station name (from CORPUS) |
| `rail_schedules` | One row per train schedule, tagged with `run_date` |
| `rail_schedule_stops` | Individual stops for each schedule (LO/LI/LT) |
| `rail_toc_metrics` | Per-operator service, route, and station counts |
| `rail_route_metrics` | Per origin→destination pair: frequency and operator count |
| `rail_station_metrics` | Per station: total calls, operator count, terminus count |
| `rail_run_metrics` | Top-level one-row-per-day summary |

All metric tables are partitioned by `run_date` using `UNIQUE (run_date, ...)` constraints, so each daily run produces an independent, replaceable snapshot. Re-running a date overwrites the existing data cleanly.

## Viewing results

Query the database directly:

```bash
kubectl exec -n default ossway-pg-1 -- psql -U app -d rail_network -c "
SELECT run_date, schedules_parsed, tocs_active, route_pairs, stations_active
FROM rail_run_metrics
ORDER BY run_date DESC
LIMIT 7;
"
```

The LLM narrative is written to the `llm_summary` task log in Airflow — look for the block delimited by `===` lines.

---

## OpenTelemetry instrumentation

This DAG is a working example of the `airflow-otel` library's instrumentation pattern. Every task emits traces, metrics, and structured log context that flow to whatever OTel collector your Airflow deployment is configured to use.

### The airflow-otel library

The library provides two main tools:

**`instrument_task_context(attributes)`**

```python
from airflow_otel import instrument_task_context, get_meter

with instrument_task_context({"nrod.feed": "CIF_FULL", "run_date": run_date}) as span:
    # all OTel calls inside here are associated with this task's trace
```

This is the entry point for every task. It:
- Creates a root span for the task, linked to the Airflow DAG run's trace context
- Injects the supplied attributes onto the root span (and, via baggage propagation, onto all child spans and metrics)
- Returns the span so you can add further attributes as the task progresses

**`get_meter(name)`**

```python
meter = get_meter("rail.fetch")
size_gauge = meter.create_gauge("rail.corpus.download_bytes", unit="By", ...)
size_gauge.set(total_bytes)
```

Returns an OTel `Meter` scoped to the given name. Use a consistent dot-notation hierarchy (`rail.fetch`, `rail.parse`, `rail.analysis`) so metrics group sensibly in dashboards.

### Airflow's built-in tracer

```python
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
```

`get_otel_tracer_for_task` returns a tracer already scoped to the current task's trace context. Child spans created from it are automatically nested under the task's root span, giving a correct hierarchy in the trace waterfall:

```
DAG run
  └─ task: fetch_corpus
       └─ rail.fetch.corpus          ← child span (download)
  └─ task: parse_schedule
       └─ rail.parse.schedule.stream ← child span (parse loop)
  └─ task: analyse_by_toc
       └─ rail.analyse.by_toc        ← parent span (all TOCs)
            └─ rail.analyse.toc.GW   ← one child span per operator
            └─ rail.analyse.toc.VT
            ...
  └─ task: llm_summary
       └─ llm.summarise_network      ← our span
            └─ openai.chat           ← OpenLLMetry auto-instrumented span
```

### Auto-instrumentation helpers

**`instrument_requests()`**

```python
from dag_utils import instrument_requests
instrument_requests()
```

Activates `opentelemetry-instrumentation-requests`, which patches the `requests` library so every outbound HTTP call automatically emits a span. The span includes the URL, HTTP method, status code, and duration. Call this once per task — the instrumentation is idempotent.

**`instrument_llm()`**

```python
from dag_utils import instrument_llm
instrument_llm()
```

Activates `opentelemetry-instrumentation-openai-v2` (OpenLLMetry). Every OpenAI SDK call — including calls to Ollama via its OpenAI-compatible API — emits spans containing:

- Model name
- Prompt and completion token counts
- Time to first token and total generation latency
- Finish reason (`stop`, `length`, or `error`)

This gives full LLM observability without any manual instrumentation of the response object.

### Choosing the right metric instrument

| Situation | Instrument | Example |
|---|---|---|
| A value that goes up or down | `Gauge` | Download size, service count per TOC |
| A value that only increases | `Counter` | Records parsed, rows inserted |

Using a `Counter` for download bytes would accumulate across re-runs of the same date, making the metric misleading. A `Gauge` always reflects the most recent value.

### Span attributes vs. log lines

Both are used in this DAG, but for different purposes:

- **Span attributes** are structured and queryable in your observability tool. Use them for values you want to filter or alert on: `cif.schedules_parsed`, `toc.service_count`, `summary.length`.
- **Log lines** are narrative context for a human reading the task log. Use them for progress updates and confirmation messages.

A good rule of thumb: if you might want to write a query like "show me all runs where schedules_parsed < 100,000", put it on the span. If you just want to confirm a step completed, log it.

### Naming conventions

Span names in this DAG follow a dot-notation hierarchy:

```
rail.fetch.corpus
rail.fetch.schedule
rail.parse.corpus
rail.parse.corpus.upsert
rail.parse.schedule.stream
rail.analyse.by_toc
rail.analyse.toc.<code>      ← one per operator
rail.analyse.by_route
rail.analyse.by_route.enrich
rail.analyse.by_station
rail.analyse.by_station.enrich
rail.aggregate
rail.store.run_metrics
llm.summarise_network
```

The hierarchy means a service graph will group all `rail.*` spans under the same service, and sub-operations are clearly distinguishable from top-level task spans.

### What the traces look like

A successful run produces a trace per DAG run with task spans running in parallel where the task graph allows it. Within each task, child spans show the internal breakdown:

- In `parse_schedule`, the `rail.parse.schedule.stream` span duration shows exactly how long the 130MB parse loop takes
- In `analyse_by_toc`, one child span per operator makes it immediately visible if a single TOC's queries are slow
- In `llm_summary`, the OpenLLMetry `openai.chat` span shows token counts and LLM latency independently of the surrounding Airflow task overhead

Metric time-series (download bytes, schedules parsed, service counts per TOC) give you trend visibility across daily runs without having to open individual traces.
