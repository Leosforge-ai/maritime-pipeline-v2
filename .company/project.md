# Project — maritime-pipeline-v2

> company-os-governed context. Canonical registry: company-os `projects.yaml`.

## What this repo is

The **AIS Voyage Engine**: a cloud-native pipeline turning raw NOAA AIS pings into enriched maritime voyage insights on a medallion architecture. **Goal: a public-facing API.**

- **Bronze** — raw AIS pings filtered by port proximity (~5km) via Polars + SciPy cKDTree.
- **Silver** — stitched port events (arrivals/departures) via dbt SQL models.
- **Gold** — enriched voyages with sea distances via dbt Python models (`searoute`).

## Stack

Python ≥3.11 (uv) · Polars · DuckDB/MotherDuck (`md:`) · dbt-duckdb · Modal (cloud compute) · Evidence.dev (Node dashboards).

## Owners (from `projects.yaml`)

Owner: Manu · Architecture: Otto · Technical lead: Sofie · **Domain lead: Logan (AIS/telematics)** · Code review: Cody · Arch review: Astrid · QA: Theo · Controller: Conrad. **All releases require Leo approval** (public API).

## Read first

`CLAUDE.md` (canonical), `GEMINI.md`, `AGENTS.md`, `docs/HISTORY.md`, `README.md`, and `.company/{repo-map,test-commands,forbidden-actions,domain-context}.md`.
