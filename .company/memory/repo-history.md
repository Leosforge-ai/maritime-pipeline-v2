# Repo History — maritime-pipeline-v2

> Durable company-os operating history. Engineering phase log: `docs/HISTORY.md`. No secrets.

## 2026-06-02 — Onboarded under company-os control (#8)

Decision: brought under company-os control via non-destructive, inventory-first onboarding. Added the `.company/` folder; **no existing files altered** (CLAUDE/GEMINI/AGENTS already present from the standards rollout). Registered in company-os `projects.yaml` (#61) as `active`; domain lead = Logan.
Evidence: company-os #60 (umbrella), maritime-pipeline-v2 #8.

## 2026-07-25 — NOAA AIS source abstraction + availability probing (#10)

Decision (Manu, with Logan's source evaluation): keep NOAA MarineCadastre (CC0) as
the batch core rather than switching providers — it didn't die, it publishes with a
multi-month lag and the old ingest script assumed "yesterday" always exists.
Live-probed `coast.noaa.gov/htdata/CMSP/AISDataHandler/` on 2026-07-25: the current
publishing year (2025) ships daily `ais-YYYY-MM-DD.csv.zst`; older years (2023,
2024) get archived into daily `AIS_YYYY_MM_DD.zip` bundles with a legacy
uppercase column schema; the 2026 directory doesn't exist yet (nothing published,
~7-month lag as of this date). Latest available date found: 2025-12-31.

Implementation: extracted an `AISSource` protocol (`src/ais_sources.py`) with
`NoaaMarineCadastreSource` handling both on-disk layouts; the KDTree port filter
and MotherDuck upload logic in `src/ingest_motherduck.py` are now source-agnostic.
Replaced the "yesterday" default with source-probed `latest_available_date()`;
added `--backfill-from` for gap backfill (skips missing days, never crashes the
run) and `--dry-run` (local DuckDB file, no `MOTHERDUCK_TOKEN` needed). Verified
end-to-end against live NOAA data in dry-run mode.

Cron re-enable is explicitly OUT of scope (Leo-gated); follow-up issue tracks
adding Finnish Digitraffic Marine as a live regional source.
Evidence: maritime-pipeline-v2 #10, PR (branch
`feat/ais-source-abstraction-noaa-probe`).
