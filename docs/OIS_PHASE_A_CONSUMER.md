# OIS Production Bundle Consumer Phase A

`OIS-PRODUCTION-BUNDLE-CONSUMER-PHASE-A-V1` is a deterministic shadow
consumer harness for `OIS-PRODUCTION-BUNDLE-MANIFEST-V1` and persisted previous
OIS state.

The harness binds the manifest to exactly these four official production
artifacts:

- `ois_status.json`
- `ois_ingestion_validation.json`
- `ois_chart_payload.json`
- `ois_chart_rolling_180.json`

It keeps Data Gate and Render Gate separate. A Data Gate pass may allow a chart
render preview only when an explicit renderer result is supplied. Missing render
evidence defaults to `NOT_EXECUTED` and fails closed. A Render Gate failure blocks
the preview and never triggers WTI/Brent refetch, MA/MACD/RSI recalculation,
rolling-180 rebuild, static fallback, or market-data fallback.

The consumer validates the previous OIS state through the canonical work-state
document validator and binds it to the portfolio and transaction ledgers in the
work-state store. Expected run, commit, production snapshot, previous-state ID,
and previous-state hash are required for acceptance; omissions fail closed.

All Phase A mutation authority fields remain false.
