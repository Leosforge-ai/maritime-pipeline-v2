# CLAUDE.md — maritime-pipeline-v2

> Conventions for Claude Code in this repo. Follows the company reusable-standards
> (company-os `templates/reusable-standards/` + `standards/`). Kept in sync with
> `GEMINI.md` and `AGENTS.md`.

## What This Repo Is

The **AIS Voyage Engine**: a cloud-native pipeline that turns raw NOAA AIS pings into
enriched maritime voyage insights, on a medallion architecture.

- **Bronze** — raw AIS pings filtered by port proximity (~5km) using **Polars** + **SciPy cKDTree**.
- **Silver** — stitched port events (arrivals/departures) via **dbt** SQL models.
- **Gold** — enriched voyages with sea distances via **dbt Python** models (`searoute`).

## Stack

- Python ≥3.11, managed with **uv** (`uv sync --group dev`, `uv run ...`).
- **Polars** for AIS processing; **DuckDB / MotherDuck** (`md:`) warehouse; **dbt-duckdb** transforms.
- **Modal** for cloud compute (`src/config.py`); **Evidence.dev** (Node) for dashboards.

---

## Hard Rules

### Response Style
- Concise; diffs over rewrites; targeted reads (`rg`); summarize output; markdown.

### Never Do
- Commit secrets/tokens (`MOTHERDUCK_TOKEN` etc.) — ever. Use `.env` / env vars.
- `uses:` a GitHub Action by mutable tag/branch — pin a commit SHA (`ci-standards`).
- Introduce **pandas** in `src/` pipeline code — AIS processing is **Polars/DuckDB-first**. Pandas is allowed **only** inside dbt Python models (`models/gold/voyages.py`) where the adapter requires a DataFrame return.
- Commit stub/placeholder functions — implement + test first. No self-merge; CI green before review.

### Always Do
- Follow `docs/WRITING_STANDARDS.md` (`type(scope): imperative`, ≤72 chars).
- Type hints on public functions; `pytest` for new behaviour. Note active tests live in `tests/test_geospacial.py` (the spelling matches `src/geospacial.py`).
- Update docs + `docs/HISTORY.md` when architecture changes.
- Confirm before any secret read/rotate/Modal-deploy operation.

---

## GitHub Issue Completion Protocol

When the task is tied to a GitHub issue: 1) save outputs; 2) branch `<type>/<slug>` (never `main`);
3) commit `type(scope): <what> — closes #<N>`; 4) `gh pr create` (body `Closes #N`, then What / Why /
Test evidence); 5) comment on the issue with summary + PR link. Skip for exploratory chats and secret ops.

## Automated PR Review

PRs are reviewed by `.github/workflows/pr-review.yml` — **Cody** (correctness/security/coverage) and
**Dalton** (SQL/data-layer QA). CRITICAL/HIGH/MEDIUM findings request changes. Replaces CodeRabbit.

## Reasoning Protocol

Apply these lenses internally before non-trivial work:

| Lens | Source | Check |
|---|---|---|
| Pre-response gate | Manu | Assumptions, scope gaps, risks ranked |
| Security | Cody | Secrets, supply-chain, unpinned actions |
| Architecture | Astrid | Structural fit, modularity, pattern breakage |
| Data engineering | Dick | PK strategy, connection hygiene (`md:`), Polars/DuckDB-first, no pandas in pipeline |
| Data QA | Dalton | Join correctness/fan-out, freshness, completeness, tenant/key integrity |

Delegate to the specialist agent for deep/risky data work; the lenses are quick scans, not replacements.
