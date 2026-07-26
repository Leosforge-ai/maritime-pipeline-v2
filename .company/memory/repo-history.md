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

## 2026-07-25 — aisstream.io live global AIS source (#14)

Decision (Leo, per Logan's evaluation): hook aisstream.io as a second,
live/global `AISSource` implementation behind the same abstraction added in
#10 — a real-time supplementary layer, not a replacement for the NOAA batch
core.

Implementation: `AisstreamSource` (`src/ais_sources.py`) connects to
`wss://stream.aisstream.io/v0/stream`, subscribes to `PositionReport` messages
(API key via `AISSTREAM_API_KEY`), and collects for a bounded window
(`--collect-seconds`, default 60) via `fetch()` — `available_dates()` /
`latest_available_date()` both resolve to "today" since this is a live-only
feed with no historical archive. Reconnects on drop within the same deadline;
never blocks past it. `src/ingest_motherduck.py` gained `--source
{noaa,aisstream}` and `--collect-seconds`, flowing through the existing
port-proximity filter and `--dry-run` local DuckDB path unchanged. New dep:
`websockets` (one new dep, pre-authorized).

Licence/status: aisstream.io is a **beta service, no SLA, no published
redistribution ToS** — per Logan's evaluation, treated as supplementary
enrichment ONLY, never the basis of the public API/product until Leila clears
the licensing ambiguity. Documented in the `AisstreamSource` docstring and
README.

Test evidence: `uv run ruff format --check . && uv run ruff check . && uv run
mypy . && uv run pytest` all green (49 tests after follow-up fixes below).

**Follow-up fixes post-review/live-smoke:**
- Cody REQUEST_CHANGES: `websockets.connect(..., open_timeout=...)`'s builtin
  `TimeoutError` on a stalled handshake wasn't in the reconnect except-tuple —
  a hung handshake killed the whole collection window instead of
  reconnecting. Fixed + added a stalled-handshake reconnect test; `ws.send()`
  also given an explicit timeout for consistency.
- Leo minted `AISSTREAM_API_KEY` (gitignored `.env`) → ran a real 60s live
  smoke (`--source aisstream --collect-seconds 60 --dry-run`, global bounding
  box — CLI doesn't expose `--bbox` yet). First run surfaced a real bug:
  aisstream's `MetaData.time_utc` is a Go `time.Time` string (trailing
  `"+0000 UTC"`), which broke the `filter_by_port_proximity` date parser
  shared with NOAA's plain timestamps. Fixed via
  `AisstreamSource._normalize_time_utc()` + parametrized tests. Re-ran clean:
  4745 `PositionReport` pings normalized in the 60s window, 220 retained
  after the KDTree port-proximity filter, 220 rows confirmed in the local
  dry-run DuckDB (Hamburg/Rotterdam/Busan/New York/LA/Antwerp/Long
  Beach/Singapore via the 12-port hardcoded fallback — UN/LOCODE scraping
  still 403s outbound, pre-existing/unrelated per #10). No MotherDuck writes.

Stacks on PR #13 (`feat/ais-source-abstraction-noaa-probe`, approved,
unmerged) — this PR's diff/base targets that branch until #13 merges.
Evidence: maritime-pipeline-v2 #14, PR #16 (branch
`feat/aisstream-live-source`).

## 2026-07-26 — Cron re-enabled with failure alerting (#10 final step)

Decision (Leo approved): re-enable the daily `pipeline.yml` schedule now that the
"yesterday" default (the April root cause — NOAA lags months behind, so
`--year/--month/--day`-less "yesterday" 404s every day) has been replaced by the
availability-aware default from #10/#13 — `resolve_dates_to_process()` with no
explicit date args probes `NoaaMarineCadastreSource.latest_available_date()` and
ingests the single most recent published date, never a blind "yesterday" guess.

Changes: `.github/workflows/pipeline.yml` — schedule restored (`0 3 * * *`
daily), a `concurrency` group added to prevent overlapping scheduled runs, and a
new `alert-on-failure` job (`needs: [etl-pipeline, deploy-pages]`, `if:
failure()`) that comments on issue #10 with the failing run URL via
SHA-pinned `actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea`
(v7.0.1). Scheduled/manual runs with no date inputs already call `uv run
python -m src.ingest_motherduck` with no args, which now resolves to the
availability-aware default — no workflow-side date logic needed.

MotherDuck/dbt/Evidence steps untouched. No pipeline execution or MotherDuck
writes from this change — Leo merges and #10 closes on the first clean
scheduled run.
Evidence: maritime-pipeline-v2 #10, PR (branch `feat/reenable-cron`).
