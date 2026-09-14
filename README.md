# OIS Technical Data Engine — Production Runtime

Data layer for ChatGPT OIS V4.4 Professional Interactive Edition.

Production execution and validation contract: [PRODUCTION_RUNTIME_SPEC.md](docs/PRODUCTION_RUNTIME_SPEC.md).
The supported entry points are `python -m src.runtime.engine` and `python -m src.runtime.publish`.

The engine provides clean, validated, repeatable futures technical data for six OIS charts:

- WTI price structure
- WTI MACD
- WTI RSI14
- Brent price structure
- Brent MACD
- Brent RSI14

It does not generate investment decisions. ChatGPT OIS consumes the JSON payload and produces the AI analysis, Evening Review, AI Paper Portfolio, and AI Technical Verdict.

## Architecture

```text
Market Data
   ↓
Source Adapter
   ↓
Rollover Normalization
   ↓
Quality Gate
   ↓
Rolling Database
   ↓
Indicator Engine
   ↓
Chart Payload
   ↓
GitHub Actions
   ↓
HTTPS Endpoint
   ↓
ChatGPT OIS V4.4
   ↓
Six Interactive Charts
```

## Data Sources

Primary futures tickers:

| Commodity | Ticker | Source |
| --- | --- | --- |
| WTI | `CL=F` | Yahoo Finance structured chart data |
| Brent | `BZ=F` | Yahoo Finance structured chart data |

Fallback architecture:

1. Yahoo Finance structured historical data via `query1.finance.yahoo.com`
2. Yahoo Finance alternative structured endpoint via `query2.finance.yahoo.com`
3. Additional reliable futures historical dataset, if configured later
4. Stooq cross-check/fallback only when contract-equivalence is documented
5. CME/ICE official verification for contract calendar and settlement review

Spot prices are explicitly excluded from technical indicator calculation. EIA WTI spot and Brent spot can be used for fundamental context or cross-checking, but they must not replace `CL=F` / `BZ=F` futures history.

## Futures vs Spot

The production technical database is futures-only. The clean CSV files are built from continuous/front-month futures representations. The engine must not splice unrelated futures series, use spot prices as a substitute, forward-fill futures closes, or invent observations.

## Rollover Logic

Yahoo `CL=F` and `BZ=F` are continuous/front-month futures representations. The engine records likely rollover or transition gaps when close-to-close moves exceed 12%.

Current normalization action:

```text
record_only_no_price_adjustment
```

No back-adjustment, manual stitching, interpolation, or forward-fill is performed. Official verification references are:

- WTI: CME settlement / contract calendar
- Brent: ICE Brent futures expiry / settlement calendar

## Indicator Definitions

Calculation conventions are fixed in `src/indicators/technical.py`.

| Indicator | Definition |
| --- | --- |
| MA20 | Simple moving average of daily closes over 20 trading days |
| MA60 | Simple moving average of daily closes over 60 trading days |
| MA120 | Simple moving average of daily closes over 120 trading days |
| MACD | DIF = EMA12 - EMA26; DEA = EMA9(DIF); Histogram = DIF - DEA |
| RSI14 | Wilder RSI using average gain/loss smoothing |

EMA seed is the first available close. Smoothing alpha is `2 / (period + 1)`.

## Quality Gate

Validation checks:

- `rows >= 370` to warm up every point in the 250-point chart payload
- `unique_dates == rows`
- `duplicate_dates == 0`
- `missing_close == 0`
- no NaN/null/impossible close values
- ascending chronological order
- no source mismatch
- abnormal jump logging
- unexpected gap logging
- completed-session freshness and exchange trading-date validation
- exactly 180 synchronized rolling rows and cross-file consistency

If validation fails, production data is not overwritten. The last PASS dataset remains active.

## Production Runtime

The single scheduled writer is `.github/workflows/ois_production.yml`, daily at
10:30 UTC / 18:30 Asia/Taipei. Manual dispatch supports a live dry run. The legacy
updater and rolling CLI are not production entry points.

The runtime stages candidates outside production, validates all four public
JSON files and persistent history, and publishes the complete snapshot in one
non-forced Git commit/ref update. Failed attempts are Actions artifacts; they
never overwrite the last valid status, validation or chart data.

See [the runtime specification](docs/PRODUCTION_RUNTIME_SPEC.md) for commands,
source finalization, schemas, tests, secrets, failure handling and rollback.

## Chart Payload Schema

Primary contract:

```text
data/production/ois_chart_payload.json
schema_version = OIS-CHART-1.0
```

Top-level fields:

- `schema_version`
- `generated_at`
- `validation_status`
- `latest_complete_source_date`
- `wti`
- `brent`
- `datasets`

Fixed datasets:

- `wti_price_structure`: `date`, `price`, `ma20`, `ma60`, `ma120`
- `wti_macd`: `date`, `dif`, `dea`, `histogram`, `zero`
- `wti_rsi`: `date`, `rsi14`, `upper70`, `middle50`, `lower30`
- `brent_price_structure`: `date`, `price`, `ma20`, `ma60`, `ma120`
- `brent_macd`: `date`, `dif`, `dea`, `histogram`, `zero`
- `brent_rsi`: `date`, `rsi14`, `upper70`, `middle50`, `lower30`

The payload is chart-ready JSON only. It does not include PNG, HTML, Plotly HTML, or rendered images.

## Support And Resistance

Support/resistance is calculated objectively and consistently:

```text
support = minimum close over trailing 20 trading days
resistance = maximum close over trailing 20 trading days
```

This algorithm is intentionally simple and stable. ChatGPT OIS may layer additional market interpretation on top, but the data engine does not change this method day to day.

## Logging

Logs are written to:

```text
logs/ois_update.log
```

Logs include timestamp, event, source/update result, validation result, and errors. API secrets must not be logged.

## GitHub Actions and Secrets

The runtime workflow replaces `ois-data-update.yml` and preserves the established
Pages and jsDelivr distribution channels. All four JSON outputs, including
`ois_chart_rolling_180.json`, are distributed. Consumers should pin the same full
Git commit across files. Separate mutable CDN requests are not snapshot-atomic.

Only the automatic `GITHUB_TOKEN` is required; the current public Yahoo adapters
need no API key. See the [runtime contract](docs/PRODUCTION_RUNTIME_SPEC.md) for
permissions and operational limitations.

## Public HTTPS Endpoints

Primary endpoints use jsDelivr and return JSON without authentication:

```text
REPOSITORY_URL=https://github.com/pili5420/OIS-Data-Engine
JSDELIVR_CHART_PAYLOAD_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_chart_payload.json
JSDELIVR_VALIDATION_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_ingestion_validation.json
JSDELIVR_STATUS_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_status.json
JSDELIVR_ROLLING_180_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_chart_rolling_180.json
```

The immutable URLs replace `@main` with the report's full 40-character
`production_commit`. Use the same commit for all three files when consuming a
fixed snapshot. Immutable URLs are verified but never purged.

GitHub Pages endpoints remain available as fallbacks:

```text
PAGES_CHART_PAYLOAD_FALLBACK_ENDPOINT=https://pili5420.github.io/OIS-Data-Engine/ois_chart_payload.json
PAGES_VALIDATION_FALLBACK_ENDPOINT=https://pili5420.github.io/OIS-Data-Engine/ois_ingestion_validation.json
PAGES_STATUS_FALLBACK_ENDPOINT=https://pili5420.github.io/OIS-Data-Engine/ois_status.json
```

The existing raw GitHub endpoints remain available as fallbacks:

```text
CHART_PAYLOAD_FALLBACK_ENDPOINT=https://raw.githubusercontent.com/pili5420/OIS-Data-Engine/main/data/production/ois_chart_payload.json
VALIDATION_FALLBACK_ENDPOINT=https://raw.githubusercontent.com/pili5420/OIS-Data-Engine/main/data/production/ois_ingestion_validation.json
STATUS_FALLBACK_ENDPOINT=https://raw.githubusercontent.com/pili5420/OIS-Data-Engine/main/data/production/ois_status.json
```

If the repository is private, do not put secrets or tokens in URLs. Use one of these safer options:

- publish only the four production JSON files to a separate public data-only repository;
- publish via GitHub Pages from a sanitized public branch;
- expose a small authenticated proxy controlled outside ChatGPT, with credentials stored server-side;
- manually upload the current production JSON files into ChatGPT project files when automation is unavailable.

## Production Ready Criteria

All checks in the [runtime specification](docs/PRODUCTION_RUNTIME_SPEC.md) must
pass: source freshness, finite complete OHLCV, unique valid trading dates, schema
and type checks, independent historical indicator verification, exactly 180
synchronized rolling rows, cross-file agreement and candidate hashes. A valid
candidate must pass Git publication dry run before the atomic production push.
