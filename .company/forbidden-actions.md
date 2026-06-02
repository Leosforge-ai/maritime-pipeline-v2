# Forbidden Actions — maritime-pipeline-v2

> Project guardrails, additional to company-os policies. Cannot be overridden at execution time.
> Owner: Manu/Otto · Reviewer: Conrad / Leila.

## Never

- Introduce **pandas** in `src/` pipeline code — AIS processing is **Polars/DuckDB-first**. Pandas is allowed **only** inside dbt Python models (`models/gold/voyages.py`) where the adapter requires a DataFrame return.
- Commit secrets/tokens (`MOTHERDUCK_TOKEN` etc.) — use `.env` / 1Password `.env.1p`; never read/commit `.env` values.
- `uses:` a GitHub Action by mutable tag/branch — pin a commit SHA.
- Commit stub/placeholder functions; self-merge; merge with red CI.
- Ship or expose the **public API** (or production/schema/Modal-deploy changes) without **Leo approval**.
- Expose vessel/operator data carrying privacy or legal risk without **Leila** review (see company-os `policies/external-platforms.md`).

## Always

- Type hints on public functions; `pytest` for new behaviour (note `tests/test_geospacial.py` matches `src/geospacial.py`).
- Follow `docs/WRITING_STANDARDS.md`; update `docs/HISTORY.md` on architecture changes.
- Prefer the `code-review-graph` MCP tools before Grep/Read when the graph is present.
- Confirm before any secret read/rotate or Modal deploy.
