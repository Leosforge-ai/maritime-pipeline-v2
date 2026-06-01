# Writing Standards — Issues, Commits, PRs

> Concise. Direct. Efficient. Canonical source: company-os `standards/writing-standards.md`.
> When the two differ, company-os wins.

## Title format (issues, commits, PRs)

```text
type(scope): imperative description
```

- **types (baseline):** `feat` `fix` `docs` `refactor` `test` `chore` `data` `style`
- **this repo's extensions:** _none_
- imperative, lowercase, no trailing period, ≤72 chars
- reference the issue: `— closes #N` in the title, or `Closes #N` in the body

## Commit trailers

```text
Co-Authored-By: Claude <model> <noreply@anthropic.com>
```

## Issue body sections (use only what applies)

`## Problem` · `## Required` · `## Scope` · `## Notes` · `## Owner`

## PR body sections

`Closes #N` · `## What changed` · `## Why` · `## Test evidence` · `## Risk / rollback`
