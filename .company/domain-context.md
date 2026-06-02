# Domain Context — maritime-pipeline-v2

> AIS / maritime-logistics domain. Owner: **Logan** (Logistics Org) · Reviewers: Otto (pipeline), Leila (privacy/legal).

## Domain

**AIS** (Automatic Identification System) vessel-tracking data from NOAA. The engine derives **port events** (arrivals/departures) and **enriched voyages** (sea distances via `searoute`) for a planned **public API**.

## Signals & rules (Logan owns)

- AIS message semantics, MMSI identity, position/heading/timestamp quality.
- Port-proximity filtering (~5km, Polars + cKDTree); dedup/gap handling; spoofing/anomaly heuristics.
- Coordinate/reference-system correctness; what the public API may expose vs withhold.

## Data architecture

Medallion: Bronze (raw, filtered AIS) → Silver (dbt port events) → Gold (dbt-python enriched voyages). Warehouse DuckDB/MotherDuck. See `docs/HISTORY.md` and the Evidence.dev dashboards (`overview.md`, `pipeline_performance.md`, `[mmsi].md`).

## Compliance framing

Vessel and operator data can carry privacy/commercial-sensitivity; a public API amplifies exposure. Coordinate API surface + any personal/operator data with **Leila** (see company-os `policies/external-platforms.md`). Source data licensing (NOAA AIS terms) is binding.
