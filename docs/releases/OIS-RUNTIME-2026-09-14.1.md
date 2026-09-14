# OIS-RUNTIME-2026-09-14.1

## Release identity

- Repository: `pili5420/OIS-Data-Engine`
- Development branch: `codex/ois-production-runtime`
- Production branch: `main`
- Engineering commit: `1ff9487e86d74d0a93ceaf085eab7450fa3d411a`.
- First production snapshot commit: `262ba06b0b37590df93aa470c4abff1bc918751f`.
- Verified GitHub Actions run: [34831472928](https://github.com/pili5420/OIS-Data-Engine/actions/runs/34831472928), completed successfully.
- Rollback point: `76f83ea5d668b5dec21152563303d87cd4313745`

## Impact analysis

The old updater modified production status/validation on failure, replaced files
one at a time, accepted incomplete daily bars, and seeded indicators from a moving
two-year download. Rolling reconciliation could rebuild state after errors.

The runtime now builds and verifies isolated candidates, persists history with a
fixed indicator seed, checks the previous rolling state, and publishes all
outputs and history using one Git commit and non-forced ref update. A failed
attempt leaves every previous production byte untouched. The new workflow takes
over the existing daily 10:30 UTC schedule; the old writer workflow is removed.
Existing Pages and jsDelivr channels include the rolling file as their fourth
output. No OIS strategy, report specification or indicator formula was changed.

## Files changed

- Added `.github/workflows/ois_production.yml`; removed `.github/workflows/ois-data-update.yml`.
- Added `src/runtime/__init__.py`, `source.py`, `engine.py`, `validation.py`, `publish.py`.
- Added `schemas/ois_runtime.schema.json`.
- Added `tests/test_runtime.py`; extended `tests/test_jsdelivr.py` to four files/eight URLs.
- Updated `scripts/prepare_pages.py`, `scripts/distribute_jsdelivr.py`, `requirements.txt` and `README.md`.
- Added `docs/PRODUCTION_RUNTIME_SPEC.md` and this release note.
- Runtime publications subsequently update the four required production JSONs,
  supporting clean CSVs/indicator JSONs and `data/runtime/history.json` atomically.

## Schema and backward compatibility

Existing four `schema_version` values, chart keys, 250-point chart payload and
180-point rolling window are retained. Adds explicit runtime contract metadata,
shared snapshot identity, source timestamps, check results and migration audit.
This is an additive schema change. Work parsers must accept these documented
new fields and should pin one full commit for all four JSON reads. Existing
indicator formulas and support/resistance/report inputs are preserved.

There is no breaking schema change in this release. Strict allowlist consumers
must extend their field allowlists as documented in the runtime specification.

## Tests and validation

- 49 unittest cases: legacy regression, source retry/error classification,
  schema/type/range, trading calendar/DST, independent Decimal historical
  indicator checks, rolling append/drop, no-new-day behavior, failure preservation,
  candidate tampering, Pages packaging, local bare-Git atomic publication and
  concurrent writer rejection.
- GitHub Actions syntax and expressions: actionlint 1.7.12, PASS.
- `git diff --check`: PASS.
- Live production dry run: PASS, `data_as_of=2026-09-11`, WTI history=504,
  Brent history=504; six rolling datasets each contain 180 valid synchronized
  dates; all four formal candidate outputs pass the runtime schema and checks.
- Atomic Git tree dry run: PASS; no production files or remote refs are modified
  by the local dry run.
- Independent verification covers every indicator value in both existing
  historical indicator files and new candidates at six-decimal output precision.
- First GitHub Actions run: `test`, `production`, `deploy-pages`, and
  `distribute-jsdelivr` all succeeded. Cloud live ingestion, candidate validation,
  Git tree dry run, atomic publication and four-file Pages packaging succeeded.
- Read-back of the actual published Git snapshot passed all runtime checks.
  Snapshot ID: `e75c3005af076ef34e8186a7789effe5097a54d4f55df9ee889861af74110323`.
  Rolling interval: `2025-12-23` through `2026-09-11`, exactly 180 rows per dataset.
- The existing Pages deployment and all four mutable/four immutable jsDelivr
  endpoints were verified successfully by the corresponding cloud jobs.

The live bootstrap comparison found two revised rows per commodity: September 10
volume, and September 11 low/close/volume. Current completed source values replace
the legacy observations during first migration, recorded under
`bootstrap_revised_rows` and `LEGACY_BOOTSTRAP_RECONCILIATION`. This is a source-data
correction, not an indicator-formula change. Later historical revisions fail.

## Secrets, failures and known issues

The workflow uses automatic `secrets.GITHUB_TOKEN`; no manually supplied provider
secret is required. Contents write permission and the existing Pages deployment
permissions/environment must be available under repository rules.

Transient source errors receive bounded retries. Critical integrity failures
return FAIL, preserve the previous valid production snapshot and retain attempt
evidence in Actions artifacts. Failed distribution is separately visible; no
failed CDN job rewrites valid production data.

Yahoo provides no formal data availability SLA. Calendar updates and genuine
historical source corrections need engineering review. An unavailable latest
holiday daily bar intentionally fails strict freshness. Public mutable URLs can
temporarily differ due to caching; immutable commit URLs are the consistent-read
contract. Scheduled execution is subject to GitHub's scheduler availability.

## Rollback method

Disable the runtime workflow, restore both production outputs and persistent
history from one known valid Git commit in a new ordinary commit, and verify
the snapshot before restoring distribution. Do not mix files from different
commits or mark stale rollback data fresh. To roll back engineering code, revert
this release commit and ensure only one writer workflow is enabled. Detailed
procedures and the pre-upgrade commit are in `PRODUCTION_RUNTIME_SPEC.md`.
