# OIS Production Runtime Specification

Contract: `OIS-RUNTIME-1.0` • Release: `OIS-RUNTIME-2026-09-14.1`

## Authorization and scope

Implements the user's explicit Production Runtime upgrade request for
`pili5420/OIS-Data-Engine`. This is data ingestion, normalization, technical
calculation, validation and runtime automation only. Existing OIS investment
strategy, report chapters, support/resistance method and decision rules are
unchanged. Further engineering-specification changes require the Chat Control
Center's `CODEX ENGINEERING CHANGE REQUEST`.

## Execution contract

The single production writer is `.github/workflows/ois_production.yml`.
The retired `.github/workflows/ois-data-update.yml` is removed to prevent two
independent writers. `ois_update.py` and `scripts/build_rolling_180.py` are legacy
utilities; they are not production entry points and must not be run against the
live checkout. Their old validation and per-file replacement do not satisfy this
runtime contract.

Schedule: daily `30 10 * * *`, **10:30 UTC / 18:30 Asia/Taipei**, including weekends.
`workflow_dispatch` supports `dry_run=true`; this performs the live pipeline,
validation, Git tree dry run and Pages packaging without publishing. Changes to
runtime code on `main` also trigger the pipeline. Pull requests only run tests.
Only `main` may publish. Concurrency serializes production runs without cancelling
an active publisher. The runner checks out current `main` after acquiring the lock
and tests that exact checkout again before ingestion.

Stages: checkout → pinned dependency installation → tests → ingestion →
normalization → MA20/MA60/MA120 → MACD → RSI14 → persistent history/rolling update
→ validation → candidate serialization → read-back validation → atomic Git tree
dry run → Pages artifact verification → atomic Git publication → independent
Pages deployment and jsDelivr distribution verification.

GitHub scheduled execution can be delayed and is not a hard real-time SLA.
GitHub can disable scheduled workflows in inactive public repositories. Workflow
run failure notifications and Actions artifacts are the runtime's operational
evidence. No separate scheduler, Work task or investment automation is created.

## Source and normalization

Preserve the existing `CL=F` WTI and `BZ=F` Brent continuous front-month Yahoo
series. Both are NYMEX futures; BZ is the NYMEX Brent Last Day Financial contract,
not ICE's B contract. Require provider metadata `instrumentType=FUTURE`,
`exchangeName=NYM` and the requested symbol. Reject a source-contract mismatch.

Use `query1.finance.yahoo.com`, falling back to query2 only after transient retry
exhaustion. Canonical source identity is `yahoo_chart` for both hosts.
Dates come from each native daily-bar timestamp in the provider's exchange
timezone. Sort order and duplicates are checked before any merge.

Trading sessions come from pinned `pandas_market_calendars`'s
`CMEGlobex_EnergyAndMetals`, including holidays and daylight saving time. A session
must have closed for at least six hours before its daily bar is eligible. This
avoids publishing intraday observations as completed days. Future dates fail.
The latest accepted source date must equal the latest eligible calendar session.
An absent latest bar, including an absent shortened-holiday bar, therefore fails
freshness rather than silently declaring the older dataset fresh.

All OHLCV arrays must have the same length as the timestamp array. Missing OHLCV,
non-finite values, booleans used as numbers, impossible OHLC ranges and negative
or fractional volumes fail. Price must be positive, preserving the current
engine's supported range. Historical negative-price contracts are outside this
baseline's supported range and need a separately controlled specification change.
No imputation, interpolation, price adjustment or duplicate-date collapse occurs.

A wholly empty bar on a calendar-confirmed non-session or shortened holiday
session may be excluded with a quality flag. Such a bar is not an effective
observation and never contributes to the 180 count. Partial missing bars and
empty normal-session bars fail. Missing sessions previously present in persistent
history also fail. Rollover gaps are recorded using the existing
`record_only_no_price_adjustment` method; no price stitching is introduced.

## Persistent history, indicators and rolling state

`data/runtime/history.json` is versioned with the production snapshot. Preserve
the complete accepted history from its original seed date; do not truncate it to
a moving two-year input window. At least 370 observations per commodity are
required to supply the existing 250-point charts with fully warmed-up MA120.

Keep existing indicator definitions exactly:

| Indicator | Calculation |
| --- | --- |
| MA20 / MA60 / MA120 | Simple mean of daily closes |
| EMA | First close seed; alpha = 2 / (period + 1) |
| MACD | DIF = EMA12 − EMA26; DEA = EMA9(DIF); histogram = DIF − DEA |
| RSI14 | Existing Wilder smoothing, including existing zero-loss convention |

For an established runtime, overlap must match all saved OHLCV values exactly.
Append only genuinely new valid sessions. Reject historical revisions,
insertions, disappearing sessions, date regressions and a source with no overlap.
The original seed remains fixed, preventing daily EMA/RSI drift.

First migration is explicit: if history state does not yet exist, load the old
clean CSVs, validate them, and reconcile their overlapping values with current
completed source bars. Record `LEGACY_BOOTSTRAP_RECONCILIATION` and per-commodity
`bootstrap_revised_rows`. This repairs legacy intraday observations once; it is
not an automatic repair path for later data-integrity failures. A malformed old
rolling state or a mismatch against its own chart payload still fails.

Read the previously committed rolling file on every established run. Retain
unchanged overlapping rows, append new synchronized valid dates and drop the
oldest excess dates. The resulting six datasets must each contain exactly 180
unique, ordered, synchronized dates. All retained indicator rows must equal
their previous values and the corresponding chart payload rows. Never rebuild
corrupt state or pad a short history to produce PASS.

No-new-day runs preserve every data row, recheck freshness and integrity, and
generate current runtime metadata. `runtime_update_result=NO_NEW_TRADING_DAY`
is informational; it does not bypass validation. The established `update_mode`
values remain compatible with the old rolling format.

## Machine-readable output contract

Formal outputs:

| Path under `data/production/` | Schema version | `record_count` |
| --- | --- | --- |
| `ois_status.json` | `OIS-STATUS-1.0` | 2 commodities |
| `ois_ingestion_validation.json` | `OIS-VALIDATION-1.0` | 2 commodity reports |
| `ois_chart_payload.json` | `OIS-CHART-1.0` | 250 dates per dataset |
| `ois_chart_rolling_180.json` | `OIS-ROLLING-180-1.0` | 180 dates per dataset |

Existing fields and six dataset names remain intact. The JSON Schema is
`schemas/ois_runtime.schema.json` (Draft 2020-12). Every formal output requires:

| Field | Type and semantics |
| --- | --- |
| `schema_version` | Existing string identifying the document format |
| `runtime_contract_version` | String `1.0` identifying these added guarantees |
| `generated_at` | RFC3339 UTC timestamp of the runtime attempt |
| `data_as_of` | ISO date, same latest complete date for both commodities |
| `source` | Object with `wti` and `brent`, each `yahoo_chart` |
| `source_timestamp` | Object with `wti` and `brent`; native latest daily-bar timestamps, RFC3339 UTC |
| `record_count` | Integer with the exact per-document meaning in the table above |
| `snapshot_id` | SHA256 of canonically serialized persistent commodity history |
| `validation_status` | `PASS` for publishable snapshots; failures are recorded outside production |
| `quality_flags` | Array of unique string codes, possibly empty |
| `missing_fields` | Array; must be empty for publication |
| `duplicate_status` | Must be `PASS` for publication |
| `freshness_status` | Must be `PASS` for publication |

`source_timestamp` is the provider's bar timestamp, not a fabricated settlement
timestamp or an alias of the fetch time. All four files share common metadata.
The existing status/validation aliases must also say PASS and agree on dates.
The two existing clean CSVs and two indicator JSONs are regenerated in the same
Git snapshot and checked against persistent history. Initial indicator warm-up
nulls are allowed only in full-history indicator files, never in the public
250/180-point datasets. JSON NaN/Infinity and duplicate object keys are rejected.

Work should parse `runtime_contract_version`, explicit field types and dataset
keys directly. Existing chart fields, 250-point chart length and 180-point rolling
length are unchanged. This is an additive contract, not a breaking schema change;
strict consumers that reject unknown fields must allow the documented additions.

## Validation and testing

The ingestion report includes explicit PASS/FAIL checks for freshness, missing
values, duplicates, trading dates, schema, type, range, indicator calculation,
rolling count, cross-file consistency and historical drift. A critical failure
aborts publication with a nonzero exit code. No exceptions are converted to PASS.

Indicator calculations are independently recomputed using Decimal arithmetic,
without calling the production indicator functions. Every historical value is
compared at the existing six-decimal output precision (tolerance 0.0000011).
Tests include fixed calendar/DST cases, formula corruption, historical revisions,
source failures, rolling append/drop continuity, schema/type/range corruption,
cross-file tampering, SHA256 tampering, offline-candidate rejection, local bare-Git
publication and concurrent writer rejection. Legacy tests run as regression tests.

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.runtime.engine --candidate data/staging/candidate-UNIQUE --report data/staging/attempt-UNIQUE.json
python -m src.runtime.publish --candidate data/staging/candidate-UNIQUE --dry-run
```

The candidate directory must be new. The engine has no direct-production-write
option. `--fixture` permits deterministic offline replay and optional `--as-of`;
these candidates are explicitly non-publishable. A replay clock cannot be used
with live ingestion. Dry runs must be described as dry runs, not production
publication.

## Atomic publication and failure handling

Build candidate files outside production, serialize and re-read them, then save a
manifest with exact file hashes and the base commit. The publisher validates the
manifest allowlist, every hash, freshness, schema, indicators and cross-file
consistency again. It uses a separate Git index to construct one complete tree
containing all four outputs, supporting files and persistent history. No series
of local `os.replace` calls is described as whole-snapshot atomicity.

Create one commit with the checked-out production commit as parent and publish
using one **non-forced** branch-ref update. A concurrent branch update rejects
the candidate; do not rebase, overwrite or force-push it. If the process crashes
before the push, the old remote snapshot remains. If the push response is lost,
compare the remote tip with the intended commit to resolve the outcome. Old Git
commits remain rollback snapshots.

Atomicity applies to the Git commit/ref and the complete Pages deployment
artifact. Separate HTTP requests to mutable `@main`, raw `main` or Pages URLs can
straddle a release or CDN cache refresh. Work must pin all four URLs to the same
full Git commit for a consistent read, and verify shared metadata/snapshot ID.
jsDelivr's four mutable and four immutable URLs are verified against Git blob
bytes; its cache propagation is not represented as an atomic operation.

Transient source failures (network/timeout, HTTP 408, 429, 500, 502, 503, 504) retry
at most three times per host, with 2/4-second backoff and bounded numeric
`Retry-After` support (maximum 60 seconds). Invalid JSON, contract mismatch,
missing values, duplicates and other integrity errors never retry into a
different source to obtain a false PASS. Source retry exhaustion fails closed.

FAIL never overwrites any previous production file, including status and
validation. Attempt reports and candidate evidence are stored in Actions
artifacts for 30 days. A downstream Pages/CDN failure marks that job failed but
does not invalidate or rewrite an already committed valid data snapshot. GitHub
keeps the previous Pages deployment on a failed deployment. Logs never contain
secret values or raw provider error bodies.

## Secrets and repository configuration

No paid provider key is required for the current public Yahoo adapters.
`secrets.GITHUB_TOKEN` is GitHub's automatically supplied token; it is not
hardcoded or manually created. The production job needs `contents: write` and
repository rules must permit its ordinary non-forced update. Do not bypass branch
protection. Pages deployment uses `pages: write` and `id-token: write` with the
existing `github-pages` environment and Pages Actions source. No PAT is required.
Future keyed sources must read an explicitly configured GitHub Actions Secret
through an environment variable; never put credentials in source, URLs or JSON.

## Rollback

Pre-upgrade rollback point: `76f83ea5d668b5dec21152563303d87cd4313745`.
For data rollback, first disable the production workflow in GitHub Actions.
Select a known valid commit and restore **both** `data/production` and
`data/runtime` from the same commit in one new Git commit, then publish that
commit with a normal push. For a pre-runtime rollback, remove the newly introduced
history state as part of the same reviewed change. Do not reset only one file.
Repackage the selected four outputs for distribution as appropriate and verify
their shared snapshot. A stale historical snapshot remains historical; do not
rewrite its freshness metadata to claim it is current.

For code rollback, revert the runtime release commit, which restores the old
workflow definition, or restore a previously tested runtime release. Do not
re-enable both production workflows. Retest and validate a fresh candidate before
enabling scheduled publication again. Legacy code does not offer this runtime's
failure guarantees, so a data-only rollback with the current runtime is preferred.

## References and operating limitations

- [GitHub schedule semantics](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [GitHub Actions Secrets](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets)
- [CME holiday and trading hours](https://www.cmegroup.com/trading-hours.html)
- [CME Brent Last Day contract](https://www.cmegroup.com/markets/energy/crude-oil/brent-crude-oil-last-day.calendar.html)
- [Market calendar package and calendar data maintenance](https://pandas-market-calendars.readthedocs.io/en/latest/)

Yahoo is an unofficial structured data endpoint with no availability SLA. The
pinned calendar is packaged reference data, not a live exchange service; new
special closures need a controlled calendar update. Genuine vendor historical
corrections intentionally stop the established runtime and require engineering
review. This runtime does not claim exchange settlement certification.
