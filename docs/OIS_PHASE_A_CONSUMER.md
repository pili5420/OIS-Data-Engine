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
render preview. A Render Gate failure blocks the preview and never triggers
WTI/Brent refetch, MA/MACD/RSI recalculation, rolling-180 rebuild, static
fallback, or market-data fallback.

All Phase A mutation authority fields remain false.
