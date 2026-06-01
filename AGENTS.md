# AGENTS.md — maritime-pipeline-v2 (Codex)

> Conventions for Codex CLI here. **Kept in sync with `CLAUDE.md` and `GEMINI.md`**;
> `CLAUDE.md` is canonical. (Product-repo convention: `AGENTS.md` is Codex's instruction file.
> The company roster lives in company-os `AGENTS.md`, a different repo.)

## What This Repo Is
The AIS Voyage Engine — NOAA AIS pings → enriched voyages on a medallion architecture
(Bronze: Polars+SciPy cKDTree; Silver: dbt SQL; Gold: dbt Python + searoute). Stack: uv-managed
Python, Polars, DuckDB/MotherDuck, dbt-duckdb, Modal, Evidence.dev.

## Hard Rules
- Never commit secrets (`MOTHERDUCK_TOKEN`). Pin GitHub Actions to a commit SHA. No committed stubs. No self-merge.
- Polars/DuckDB-first for AIS processing; **no pandas in `src/`** (pandas only inside dbt Python models).
- Type hints + `pytest` for new behaviour. Follow `docs/WRITING_STANDARDS.md`. Update `docs/HISTORY.md` on architecture changes.
- Confirm before any secret/Modal-deploy operation.

## GitHub Issue Completion Protocol
Save outputs → branch `<type>/<slug>` (never `main`) → commit `type(scope): <what> — closes #N` → `gh pr create` (body `Closes #N`, then What / Why / Test evidence) → comment on the issue. Skip for exploratory work and secret ops.

## Automated PR Review
`pr-review.yml` runs Cody (+ Dalton for data-layer) ; CRITICAL/HIGH/MEDIUM request changes. (Replaces CodeRabbit.)

## Reasoning Protocol
Apply internally: **Manu** (assumptions/scope/risk), **Cody** (security/unpinned actions), **Astrid** (architecture), **Dick** (PK strategy, `md:` connection hygiene, Polars-first), **Dalton** (data QA).
