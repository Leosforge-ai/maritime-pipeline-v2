# GEMINI.md — maritime-pipeline-v2 (Gemini CLI)

> Conventions for Gemini CLI here. **Kept in sync with `CLAUDE.md` and `AGENTS.md`**;
> `CLAUDE.md` is canonical. Gemini operating rules: company-os `integrations/gemini-cli.md` + `THE_WAY.md`.

## What This Repo Is
The AIS Voyage Engine — NOAA AIS pings → enriched voyages on a medallion architecture
(Bronze: Polars+SciPy cKDTree; Silver: dbt SQL; Gold: dbt Python + searoute). Stack: uv-managed
Python, Polars, DuckDB/MotherDuck, dbt-duckdb, Modal, Evidence.dev.

## Gemini CLI operating rules (read-only research)
- Context-anchor (list dir) → grep-first → surgical reads. Never full files >100 lines without reason.
- Research only: never make code changes, commits, or PRs (that's Claude/Codex).

## Hard Rules
- Never commit secrets (`MOTHERDUCK_TOKEN`). Pin actions to a SHA. No committed stubs. No self-merge.
- Polars/DuckDB-first; no pandas in `src/` (pandas only in dbt Python models).
- Follow `docs/WRITING_STANDARDS.md`; update `docs/HISTORY.md` on architecture changes.

## GitHub Issue Completion Protocol
Same as `CLAUDE.md`: save → branch `<type>/<slug>` → commit `— closes #N` → `gh pr create` (`Closes #N`) → comment with summary + PR link. Skip for exploratory work and secret ops.

## Automated PR Review
`pr-review.yml` runs Cody + Dalton; CRITICAL/HIGH/MEDIUM request changes. (Replaces CodeRabbit.)

## Reasoning Protocol
Lenses: **Manu** (assumptions/scope/risk), **Cody** (security), **Astrid** (architecture), **Dick** (PK strategy, `md:` connection hygiene, Polars-first), **Dalton** (data QA). Delegate to specialists for deep work.
