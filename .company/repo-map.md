# Repo Map — maritime-pipeline-v2

> Inventory for company-os onboarding (#8). Non-destructive: records what exists.
> Logan (domain) + Rico refresh at the start of significant work orders.

## Layout

| Path | What |
|---|---|
| `src/` | Python pipeline — Bronze AIS filtering (Polars + SciPy cKDTree), config (`src/config.py`, Modal), `src/geospacial.py` |
| `models/` | dbt models — Silver (port events, SQL) + Gold (`gold/voyages.py`, dbt Python + searoute) |
| `dbt_project.yml`, `profiles.yml`, `packages.yml` | dbt-duckdb project config |
| `tests/` | pytest (active: `tests/test_geospacial.py`) |
| Evidence.dev | `package.json` (Node) dashboards; `overview.md`, `[mmsi].md`, `index.md`, `pipeline_performance.md`, `evidence.plugins.yaml` |
| `docs/` | `HISTORY.md`, `WRITING_STANDARDS.md` |
| `reports/` | generated reports |

## Stack & tooling

Python ≥3.11 (uv, `uv sync --group dev`) · Polars · DuckDB/MotherDuck · dbt-duckdb · Modal · Evidence.dev (Node). Lint `ruff`, types `mypy`, tests `pytest`.

## Data architecture (medallion)

NOAA AIS pings → Bronze (Polars port-proximity filter) → Silver (dbt port events) → Gold (dbt-python enriched voyages, searoute). Warehouse: DuckDB/MotherDuck (`md:`).

## CI / hooks / config (preserve — do not alter without a work order)

`.github/workflows/`: `ci.yml`, `pipeline.yml`, `pr-review.yml`, `pre-commit-gitleaks.yml`, `add-to-project.yml`. Plus `.pre-commit-config.yaml`, `.githooks/`, `.gitleaks.toml`, `.markdownlint-cli2.jsonc`, `.mcp.json` (code-review-graph MCP), `.devcontainer/`, `pyproject.toml`, `uv.lock`, `package.json`/`package-lock.json`.

## Environments & secrets (names only — never read/commit values)

`.env` (gitignored) + `.env.1p` (1Password `op://` refs) + `.env.example`. Key secret: `MOTHERDUCK_TOKEN`. Inject via `op run --env-file=.env.1p -- <cmd>`. Modal + GitHub Actions org secrets back cloud/CI.
