# Project History — maritime-pipeline-v2

> Factual record of phases, decisions, and pivots. Newest at the bottom.
> Follows the company HISTORY.md standard (company-os `standards/history-standards.md`).

## Phase 0 — AIS Voyage Engine bootstrap (pre-2026-06-01)

**Context:** Build a cloud-native pipeline turning raw NOAA AIS pings into enriched voyages.
**Decisions:** Medallion architecture — Bronze (Polars + SciPy cKDTree port-proximity filter),
Silver (dbt SQL port-event stitching), Gold (dbt Python voyage enrichment via `searoute`);
MotherDuck/DuckDB warehouse; Modal cloud compute; Evidence.dev dashboards.
**Pivots:** None recorded.
**Outcome:** Working AIS → voyages pipeline.

## Phase 1 — Company standards adoption (2026-06-01)

**Context:** Adopt the company reusable-standards (company-os #13, Phase 3).
**Decisions:** Add AI-instruction files (CLAUDE.md/GEMINI.md/AGENTS.md) and writing/history
standards; migrate tooling to **uv + ruff + mypy** (from setuptools + black/flake8/isort);
secret scanning via the **gitleaks CLI**; automated **Cody + Dalton** PR review (replacing
CodeRabbit); pinned-SHA CI. Dependency audit (Rico) found the set already lean — added the
missing `modal` declaration; kept `pyarrow` (interop). dbt + Evidence toolchains untouched.
**Pivots:** Tooling only — no pipeline logic change.
**Outcome:** Repo aligned with company-os standards.
