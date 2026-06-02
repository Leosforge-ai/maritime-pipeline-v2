# Test & CI Commands — maritime-pipeline-v2

> Owner: Sofie · Reviewer: Cody/Theo.

## Local

```bash
uv sync --group dev          # install deps (uv only)
uv run pytest                # tests (active: tests/test_geospacial.py)
uv run ruff check .          # lint
uv run mypy .                # type check
uv run pre-commit run --all-files
# dbt (DuckDB/MotherDuck)
uv run dbt build             # or dbt run / dbt test
# Evidence.dev dashboards (Node)
npm run sources && npm run dev
```

## CI (GitHub Actions, pinned SHAs)

- `ci.yml` — ruff, mypy, pytest.
- `pipeline.yml` — pipeline run/checks.
- `pre-commit-gitleaks.yml` — pre-commit + gitleaks.
- `pr-review.yml` — automated Cody review (privacy/correctness/security); CRITICAL/HIGH/MEDIUM request changes.

CI green before review; no self-merge. Confirm before any secret read/rotate/Modal-deploy.
