# RATE Production Data Layer Specification

Contract: `RATE-PRODUCTION-1.0` • Runtime contract: `1.0`

This layer supplies machine-readable evidence and market inputs. It does not
assign Buy/Sell decisions and does not modify Stage definitions, Rotation logic,
Portfolio rules, M7/MHE decision rules or report chapters.

The formal datasets are `market_structure`, `weekly_structure`,
`margin_financing`, `institutional_flow`, `smart_money`,
`fundamental_evidence`, `top50_universe`, `top30_universe`, `stage_inputs`,
`rotation_inputs`, `m7_inputs`, `mhe_inputs`, and `portfolio_market_data`.
Every row has an exact field set defined in `src/rate/contract.py`; evidence rows
use explicit `evidence_refs` rather than prose interpretation.

The three production JSON contracts are:

- `data/rate/production/rate_status.json`
- `data/rate/production/rate_ingestion_validation.json`
- `data/rate/production/rate_data_payload.json`

Every file contains `schema_version`, `contract_version`, `generated_at`,
`data_as_of`, `source`, `source_timestamp`, `record_count`,
`validation_status`, `quality_flags`, `missing_fields`, `duplicate_status`, and
`freshness_status`. PASS files also contain a shared `snapshot_id`.

The source bundle is a JSON object with `metadata` and `datasets`. The source
must provide all 13 datasets. `top50_universe` must contain exactly 50 rows and
`top30_universe` exactly 30 rows. Each dataset rejects missing values, unknown or
missing fields, duplicates, non-finite numbers, invalid ranks, invalid evidence
references, future timestamps and data older than seven days. Cross-file metadata
and snapshot identity must agree.

The workflow `.github/workflows/rate_production.yml` runs on weekdays at 11:00
UTC, supports manual dry runs, serializes writers and publishes only a PASS
candidate. It reads `RATE_SOURCE_URL` and optional `RATE_SOURCE_TOKEN` from
GitHub Actions Secrets. Neither is embedded in source code. If the source secret
is not configured, the run fails closed and does not create or overwrite RATE
production data. Network retry and provider-specific adapters must be added only
through a controlled engineering change with an explicit source contract.

This repository now contains the RATE contract, validator, candidate generator,
publisher, workflow and tests. It does not claim RATE validation PASS until an
authorized source bundle is configured; fabricating Top50, Top30, M7, MHE, Stage
or Rotation values would violate the data-engineering contract.
