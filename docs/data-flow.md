# Data Pipeline Flow — ER-Accelerator

## Pipeline Stages

The entity resolution pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold):

```
  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
  │ INGEST   │ →  │ CLEANSE  │ →  │  MATCH   │ →  │ SURVIVE  │ →  │  EXPORT  │
  │ (Bronze) │    │ (Silver) │    │ (Silver) │    │  (Gold)  │    │  (Gold)  │
  └──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

### Stage 1: Ingestion (Bronze)
- **Backend**: `engine/ingestion.py` — `IngestionFactory` selects connector (JDBC or File)
- **Frontend**: `inspector.py` Ingestion stage — table/source selection
- **Output**: Raw data with `_ingest_timestamp` and `_source_system` metadata columns
- `ColumnNormalizer` standardizes headers to snake_case

### Stage 2: Cleansing (Silver)
- **Backend**: `engine/cleaning.py` — `DataCleaner` with Pandas UDFs
- **Available UDFs**: `clean_text_udf`, `clean_phone_udf`, `generate_soundex_udf`
- **Frontend**: `inspector.py` Cleansing stage — rule selection per column
- **Config storage**: `bronze_configuration` JSON column in `configuration_metadata` table

### Stage 3: Matching (Silver)
- **Backend**: `engine/matching.py` — `MatchingEngine.run_waterfall()`
- **Strategy**: Multi-pass waterfall blocking with salted joins for skew handling
- **Frontend**: `inspector.py` Matching stage — blocking key + method config
- Unmatched records from Pass N cascade to Pass N+1

### Stage 4: Survivorship (Gold)
- **Backend**: `governance/survivorship.py` — `SurvivorshipEngine`
- **Strategy**: Trust Score → Completeness → Recency ranking per cluster
- **Modes**: Row-level (simple) or Field-level (advanced, per-column best value)

### Stage 5: Export (Gold)
- Golden Records exported to Unity Catalog Gold tables or CSV

## configuration_metadata Table

This Databricks table persists all connector and pipeline config:

| Column | Content |
|--------|---------|
| `connection_id` | UUID |
| `connector_type` | `sqlserver`, `databricks`, `fabric` |
| `source_configuration` | JSON — connection params |
| `tbl_configuration` | JSON — selected schemas/tables |
| `bronze_configuration` | JSON — cleansing rules |
| `silver_configuration` | JSON — matching rules (future) |

## Frontend ↔ Backend Mapping

| Inspector Stage | Backend Module | Key Class/Function |
|----------------|----------------|-------------------|
| Ingestion | `engine/ingestion.py` | `ingest_source()` |
| Cleansing | `engine/cleaning.py` | `DataCleaner.apply_cleaning_rules()` |
| Matching | `engine/matching.py` | `MatchingEngine.run_waterfall()` |
| Survivorship | `governance/survivorship.py` | `SurvivorshipEngine.resolve_conflicts()` |
| Export | `connector_service.py` | Direct Spark write to Delta |
