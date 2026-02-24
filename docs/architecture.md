# Architecture — ER-Accelerator

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Streamlit Frontend (app.py)                                │
│  ┌──────┐ ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌───────┐ │
│  │Connec│ │ Inspector│ │Dashboard│ │Match Rev │ │ Audit │ │
│  │ tors │ │ Pipeline │ │         │ │  iew     │ │ Logs  │ │
│  └──┬───┘ └────┬─────┘ └─────────┘ └────┬─────┘ └───────┘ │
│     │          │                         │                  │
│     ▼          ▼                         ▼                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              ConnectorService (singleton)             │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │ Adapters: SQLServer │ Databricks │ Fabric      │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                   │
│  ┌──────────────────────┴───────────────────────────────┐   │
│  │ Engine: Ingestion → Cleaning → Matching → Survivorship│   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                   │
│  ┌──────────────────────┴───────────────────────────────┐   │
│  │ Cross-cutting: AuditLogger │ UserManager │ Registry   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────┬───────────────────────────────┘
                              │ Databricks Connect / Local Spark
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Databricks Workspace                                       │
│  Unity Catalog → Bronze / Silver / Gold Delta Tables        │
│  Volumes → File storage (landing zone)                      │
└─────────────────────────────────────────────────────────────┘
```

## Adapter Pattern (Connectors)

All connectors implement `BaseConnectorAdapter` (Strategy pattern):

```python
# To add "Snowflake", create:
class SnowflakeAdapter(BaseConnectorAdapter):
    connector_type = "snowflake"     # unique ID
    display_name = "Snowflake"       # UI label
    required_fields = [...]          # form fields
    def build_jdbc_url(self, config): ...
    def fetch_schemas_and_tables(self, spark, config): ...
    def test_connection(self, spark, config): ...
    def fetch_columns(self, spark, config, schema, table): ...
```

Registration chain: `adapters/__init__.py` → `ConnectorService._register_adapters()`.

## Singletons

| Service | File | Factory |
|---------|------|---------|
| `ConnectorService` | `connectors/connector_service.py` | `get_connector_service()` |
| `Bootstrapper` | `bootstrapper.py` | `get_bootstrapper()` |
| `AuditLogger` | `audit/logger.py` | Direct instantiation, cached in session |
| `MetadataRegistry` | `metadata/registry.py` | `get_registry()` |

## Session State (Streamlit)

Key session state variables managed in `app.py`:
- `authenticated`, `user_role`, `user_name` — auth state
- `current_page` — active view routing
- `ingestion_connector_config` — selected connector + tables
- `PLATFORM_USERS` — user store

## Configuration Files

| File | Purpose |
|------|---------|
| `config/system_config.yaml` | Spark app name, storage paths, governance thresholds |
| `config/semantic_types.yaml` | Regex definitions for PII types (NPI, SSN, Email, etc.) |
| `.streamlit/secrets.toml` | Databricks host, cluster ID, token (**gitignored**) |
