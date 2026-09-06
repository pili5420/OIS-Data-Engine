# OIS Technical Data Engine v1

Data layer for ChatGPT OIS V4.4 Professional Interactive Edition.

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

- `rows >= 250`
- `unique_dates == rows`
- `duplicate_dates == 0`
- `missing_close == 0`
- no NaN/null/impossible close values
- ascending chronological order
- no source mismatch
- abnormal jump logging
- unexpected gap logging

If validation fails, production data is not overwritten. The last PASS dataset remains active.

## Initialization

Run:

```bash
python ois_update.py --initialize
```

Preferred baseline history is at least 500 trading days for each commodity. The minimum acceptable production baseline is 250 trading days.

Production files:

```text
data/production/ois_wti_clean.csv
data/production/ois_brent_clean.csv
data/production/ois_wti_indicators.json
data/production/ois_brent_indicators.json
data/production/ois_chart_payload.json
data/production/ois_ingestion_validation.json
data/production/ois_status.json
```

## Daily Updater

Run:

```bash
python ois_update.py
```

Other modes:

```bash
python ois_update.py --force
python ois_update.py --validate-only
python ois_update.py --initialize
```

Daily behavior:

1. Fetch latest structured futures data.
2. Keep previous PASS dataset if source fetch fails.
3. Detect no-new-complete-trading-day.
4. Validate staging data.
5. Calculate indicators.
6. Generate chart payload.
7. Atomic replace production only on PASS.
8. Archive a PASS snapshot under `data/archive/YYYY-MM-DD/`.

Weekend and holiday behavior returns `NO_NEW_COMPLETE_TRADING_DAY` and keeps the latest PASS production dataset usable.

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

## GitHub Actions

Workflow:

```text
.github/workflows/ois-data-update.yml
```

It runs daily and supports manual `workflow_dispatch`.

The schedule is 10:30 UTC (18:30 Asia/Taipei), before the 20:00 OIS review.
Manual runs can enable `force_update` to verify the complete publication path;
scheduled runs retain the default no-new-trading-day behavior.

The workflow:

1. Checks out the repository.
2. Sets up Python.
3. Installs dependencies.
4. Runs `python ois_update.py`.
5. Runs tests.
6. Runs `python ois_update.py --validate-only`.
7. Commits and pushes only when production data changed.
8. Verifies PASS status and consistent source dates, then packages the three public JSON files.
9. Deploys the artifact to GitHub Pages after the update job succeeds.
10. Independently purges and verifies jsDelivr URLs for the post-push production commit.

The update job captures `git rev-parse HEAD` after the production push. The
jsDelivr job checks out that exact commit, generates `@main` and full-SHA URLs
for all three JSON files, and purges each `@main` cache. All six GET responses
must return HTTP 200, `application/json`, and the same SHA256 as the committed
Git blob. Pending purge requests are polled; provider failures, throttling, and
stale content are reported as failures. Verification uses the canonical URLs,
without cache-busting query parameters.

Every run lists all six concrete URLs in its Actions summary and preserves
`ois_jsdelivr_distribution.json` in the `ois-jsdelivr-distribution-<run>-<attempt>`
artifact. The report records the production commit, purge results, response
checks, and Pages/raw fallback URLs. It is separate from production JSON, so
there is no schema change or self-referencing commit. A jsDelivr failure does
not block the independent GitHub Pages deployment job.

Pages publication uses `scripts/prepare_pages.py` to copy the original JSON bytes.
It does not recalculate indicators, rewrite the database, or change any schema.
An updater, test, validation, or packaging failure prevents Pages deployment and
leaves the previous Pages deployment available. No-new-trading-day runs publish
the retained dataset after production validation confirms PASS.

No new data means no meaningless commit. Failed validation means corrupted data is not committed.

## Secrets

No API keys are hardcoded. Optional future adapters must use environment variables or GitHub Actions Secrets, for example:

- `STOOQ_API_KEY`
- `NASDAQ_API_KEY`

The engine must continue safely without optional API keys by preserving the last PASS production dataset.

## Public HTTPS Endpoints

Primary endpoints use jsDelivr and return JSON without authentication:

```text
REPOSITORY_URL=https://github.com/pili5420/OIS-Data-Engine
JSDELIVR_CHART_PAYLOAD_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_chart_payload.json
JSDELIVR_VALIDATION_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_ingestion_validation.json
JSDELIVR_STATUS_ENDPOINT=https://cdn.jsdelivr.net/gh/pili5420/OIS-Data-Engine@main/data/production/ois_status.json
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

- publish only the three production JSON files to a separate public data-only repository;
- publish via GitHub Pages from a sanitized public branch;
- expose a small authenticated proxy controlled outside ChatGPT, with credentials stored server-side;
- manually upload the current production JSON files into ChatGPT project files when automation is unavailable.

## Production Ready Criteria

The engine is Production Ready only when all checks pass:

- WTI rows >= 250
- Brent rows >= 250
- duplicate dates = 0
- missing Close = 0
- MA120 valid
- MACD valid
- RSI14 valid
- chart payload schema PASS
- overall validation PASS
- GitHub Actions workflow present
- public HTTPS endpoints configured for the target repository
