Run an automated review of a pull request as Cody (code correctness + security + coverage)
and Dalton (SQL/data-layer QA). Mirrors `.github/workflows/pr-review.yml` so you can review
locally before CI does.

## Usage

`/pr-review [PR_NUMBER]`

If PR_NUMBER is omitted, detect the open PR for the current branch:
```bash
gh pr view --json number -q .number
```

## Steps

1. Resolve the PR number (argument or the command above); REPO = `Leosforge-ai/maritime-pipeline-v2`.
2. Get changed files: `gh api repos/{REPO}/pulls/{PR}/files`. Review only code
   (`.py .sql .yml .yaml .toml`); skip docs/images. If none, report "No reviewable code" and stop.
3. **Cody pass** — correctness, security (secrets incl. `MOTHERDUCK_TOKEN`, supply-chain,
   unpinned actions), test-coverage gaps. Cite `file:line`.
4. **Dalton pass** — SQL/dbt + data-layer Python: join correctness & fan-out, PK strategy
   (missing PK = CRITICAL), `md:` connection hygiene, no pandas in `src/` (Polars/DuckDB-first;
   pandas only in dbt Python models), freshness/completeness.
5. Aggregate under CRITICAL / HIGH / MEDIUM / LOW. REQUEST_CHANGES if any CRITICAL/HIGH/MEDIUM, else APPROVE.

No false positives — verify each finding against the actual code.
