# OIS Production Bundle Manifest V1

`OIS-PRODUCTION-BUNDLE-MANIFEST-V1` is an additive shadow binding contract for
the four official production files:

- `ois_status.json`
- `ois_ingestion_validation.json`
- `ois_chart_payload.json`
- `ois_chart_rolling_180.json`

The manifest does not replace, rename, or redefine those files. It references
their hashes and shared production metadata only. It does not recalculate WTI,
Brent, MA, MACD, RSI, or rolling-180 payloads.

Required validation bindings:

- `production_snapshot_id`
- `run_id`
- `commit_sha`
- `generated_at`
- validation and freshness status
- payload references for exactly the four official artifacts
- previous-state requirement
- blocked dependencies

Validation is fail closed. Missing artifacts, invalid snapshot binding, commit
or run mismatch, stale/future artifact timestamps, validation/freshness failure,
corrupted manifest hash, missing public artifact references, and invalid
previous-state requirements return `FAIL_CLOSED` and disallow state, portfolio,
and ledger mutation.
