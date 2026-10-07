# Source Dataset Health Report — NYC 311 Service Requests

**Prepared by:** Abdul Rehman
**Date:** 2026-08-04
**Dataset:** NYC 311 Service Requests (NYC Open Data / Socrata, dataset id `erm2-nwe9`)
**Scope:** Phase 1 — raw data landed in S3 (no cleaning/transformation applied yet)

## 1. Executive Summary

The source dataset satisfies the project's requirement for both a historical batch and a live incremental feed, and is confirmed to be realistically noisy — not a clean teaching dataset. All data landed so far (the original 876,414-row CSV for Jan–Mar 2025, plus a one-time API-sourced backfill covering Apr 2025–Aug 2026, ~5.1M records across 104 files/7.3GB) is static and closed, so all of it is classified as **historical**. The **incremental** feed proper has not landed anything yet — it starts fresh from 2026-08-03 on the automated daily schedule. The original CSV has been profiled in full; no data corruption or loss was found during ingestion of the backfill. Several concrete data-quality issues were identified and are documented below with measured rates, not estimates — all deferred to the cleaning/transform phase per project scope.

## 2. Dataset Identity & Sourcing

| | |
|---|---|
| Source | NYC Open Data (Socrata), dataset `erm2-nwe9`, "311 Service Requests from 2010 to Present" |
| Historical acquisition (original) | Manual UI export (CSV), Jan 1 – Mar 31 2025 |
| Historical acquisition (API backfill) | Live Socrata API, pulled via AWS Lambda, manually re-invoked ~104 times to close the gap between the historical cutoff and today — a one-time catch-up, not a recurring pull |
| Incremental acquisition (ongoing) | Same Socrata API/Lambda mechanism, but on an automated daily schedule — starts fresh from 2026-08-03, nothing landed under this yet |
| Total date range landed | 2025-01-01 through 2026-08-02, entirely classified as historical (static, closed ranges); the live incremental feed has not landed anything yet |
| Landing format | Historical: raw CSV as-is (original) + JSON Lines (API backfill portion). Incremental (once it starts): JSON Lines |
| Storage location | S3 raw zone — `raw/311/historical/` (both the original CSV and the API backfill) and `raw/311/incremental/` (currently empty, awaiting the first live daily run) |

## 3. Volume

| Segment | Rows/Records | Size | Notes |
|---|---|---|---|
| Historical — original CSV | 876,414 | 591.5 MiB (620MB on disk) | Full population, profiled below |
| Historical — API backfill (Apr 2025–Aug 2026) | ~5.1M (104 files) | ~7.3GB | One-time catch-up, fetched via the incremental Lambda's mechanism but classified as historical since it's a static, closed range — see `decisionlog.md` #18 |
| Live incremental feed | 0 so far | 0 | Starts fresh 2026-08-03, ~10,000-11,000 records/day expected going forward |

**Ingestion integrity check:** AWS CloudWatch recorded 107 total Lambda invocations during the catch-up; exactly 3 failed (a handler configuration bug, a schema mismatch, and one out-of-memory error — all encountered and fixed early, before any data was written) and 104 succeeded, matching the 104 distinct files verified in S3 byte-for-byte. No silent data loss.

## 4. Data Quality Findings — Historical File (Full Population, All 876,414 Rows)

Null/blank rate per column (0% and near-0% columns omitted from discussion, shown for reference):

| Field | Blank Rate | Assessment |
|---|---|---|
| `Due Date` | 99.5% | Effectively unused field — not a data quality defect, just rarely populated |
| `Taxi Company Borough` | 99.9% | Only applicable to taxi-related complaints |
| `Taxi Pick Up Location` | 99.1% | Same |
| `Vehicle Type` | 96.1% | Only applicable to vehicle-related complaints |
| `Facility Type` | 92.4% | Only applicable to facility-related complaints |
| `Bridge/Highway *` fields | 99.5–99.8% | Only applicable to bridge/highway complaints |
| `Park Facility Name` (literal "Unspecified") | 99.7% | Placeholder value, not a true null — needs explicit handling |
| `Landmark` | 44.1% | Frequently blank |
| `Additional Details` | 50.7% | Frequently blank |
| `Intersection Street 1/2` | 38.0% | Only applicable to intersection-type addresses |
| `Cross Street 1/2` | ~34.9% | Only applicable to street-type addresses |
| `Location Type` | 11.3% | Moderate blank rate |
| `BBL` | 9.2% | Missing for non-address-based location types |
| `X/Y Coordinate, Latitude, Longitude, Location` | 1.3% | Missing entirely for ~11,067 rows (non-geocodable addresses) |
| `Closed Date` | 1.2% | Expected — still-open complaints |
| `Council District` | 1.7% | Minor gaps |
| `Unique Key`, `Created Date`, `Agency`, `Agency Name`, `Complaint Type`, `Status`, `Community Board`, `Police Precinct`, `Borough`, `Open Data Channel Type`, `Park Borough` | 0.0% | Fully populated |

**Key finding:** most blanks are *conditional*, not missing data — a cleaning pass must treat field applicability per complaint type, not blanket-impute or flag every blank as an error.

**Placeholder values disguised as data (measured):**
- `Community Board` contains the literal string `"Unspecified <BOROUGH>"` instead of a real board number in **9,108 rows (1.0%)**.
- `Park Borough` = `"Unspecified"` in **549 rows (0.1%)**.
- `Park Facility Name` = `"Unspecified"` in **873,863 rows (99.7%)** — expected, since most complaints aren't park-related, but the string masks as valid data rather than a clean null.

**Format issues (measured):**
- `X Coordinate (State Plane)` (and `Y Coordinate`) are stored as comma-formatted text in **865,349 of 876,414 populated rows (98.7%)** — e.g. `"1,016,915"` — breaking naive numeric parsing without a strip step first.
- Dates are stored as `MM/DD/YYYY HH:MM:SS AM/PM` text strings, not ISO format, across all populated date fields.

**Duplication check:** zero duplicate `Unique Key` values found across all 876,414 historical rows — the historical file itself is internally clean on this dimension. (Note: this does not cover duplication *across* the historical file and the separate monthly exports — see §5.)

## 5. Structural / Cross-Source Issues

- **Historical file total vs. component monthly exports disagree by ~44,000 rows** — the three separate monthly exports (Jan/Feb/Mar) sum to 832,316 rows, but the merged file covering the identical range has 876,414. This reflects that the source is a live, continuously-updated view, not a frozen snapshot — a genuine real-world data instability finding.
- **Schema mismatch between the original CSV and the API-sourced data** (both the historical backfill and the future incremental feed use the same API format): the CSV uses human-readable column names (`"Problem (formerly Complaint Type)"`); the API returns snake_case machine field names (`complaint_type`). Not reconciled at landing time by design (raw-zone fidelity).
- **No `updated_date` field exists** in the live API at all (only `created_date`, `closed_date`, `resolution_action_updated_date`) — discovered via direct API testing, not assumed. This means any API-based pull (the backfill already done, and every future incremental run) can only detect *new* complaints, not existing ones later updated with no new row.
- **A field drift**: the live API includes `descriptor_2`, which has no corresponding column in the historical CSV's 41-column header — schema drift between when the historical export was taken and now.
- **Structural type mismatch**: the historical CSV's `Location` field is a string (`"POINT (...)"`); the live API returns it as a nested JSON object (`{"type": "Point", "coordinates": [...]}`)  — needs unification before joint use.

## 6. What's Explicitly Not Assessed Yet

- The API-sourced historical backfill (~5.1M records) has not been profiled at the same field-level depth as the original CSV — its volume and successful landing are verified, but per-field null/format rates have not been computed.
- The live incremental feed hasn't started yet (first automated run expected 2026-08-04 06:00 UTC), so there is nothing to assess there at all.
- No cross-referential validation yet (e.g., whether every `Closed Date` postdates its `Created Date`, or whether `Status` values are consistent with presence/absence of `Closed Date`).
- Free-text truncation (`Resolution Description` cutting off mid-sentence, seen in sampled rows) has not been quantified — how many rows are affected is currently unknown.

## 7. Bottom Line

The dataset meets the project's stated requirements: a genuine historical + incremental structure, backed by measured (not assumed) real-world noise — conditional nulls, placeholder values, unstable row counts across pulls, comma-formatted numbers, and schema drift between sources. All of this is landed as-is in the S3 raw zone per Phase 1 scope; no cleaning has been applied. Recommendations for how to address these findings (transform tooling, schema reconciliation approach, data-quality automation) are addressed in the later phases (see `Architecture.md`).
