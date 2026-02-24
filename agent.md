# ER-Accelerator — AI Agent Instructions

## Identity
Enterprise Entity Resolution Accelerator by iLink Systems.  
A Streamlit UI over PySpark/Databricks that ingests, cleans, matches, and merges records into Golden Records.

## Tech Stack
- **Frontend**: Streamlit (Python 3.12), CSS-in-Python via `st.markdown(unsafe_allow_html=True)`
- **Backend**: PySpark, Databricks Connect, Unity Catalog (Medallion: Bronze → Silver → Gold)
- **Config**: YAML (`config/`), TOML (`.streamlit/secrets.toml`)
- **Run**: `streamlit run src/frontend/app.py`

## Project Structure
```
src/
├── frontend/
│   ├── app.py                  # Main Streamlit app, routing, CSS, auth
│   └── views/                  # Each file exports render()
│       ├── connectors.py       # Data source connection UI
│       ├── inspector.py        # Pipeline Inspector (Ingestion→Cleansing→Matching→Export)
│       ├── dashboard.py        # Executive dashboard with charts
│       ├── match_review.py     # Side-by-side record comparison & resolution
│       ├── steward_workbench.py# Data steward queue (basic)
│       ├── audit.py            # Audit log viewer
│       └── users.py            # User management (RBAC)
├── backend/
│   ├── bootstrapper.py         # Spark session init (Databricks Connect or local)
│   ├── connectors/
│   │   ├── connector_service.py # Central orchestration singleton
│   │   └── adapters/           # Strategy pattern — one adapter per DB type
│   │       ├── base_adapter.py # Abstract base (BaseConnectorAdapter)
│   │       ├── databricks_adapter.py
│   │       ├── sql_server_adapter.py
│   │       └── fabric_adapter.py
│   ├── engine/
│   │   ├── ingestion.py        # SourceConnector ABC, IngestionFactory, ColumnNormalizer
│   │   ├── cleaning.py         # DataCleaner (Pandas UDFs for text/phone/soundex)
│   │   ├── matching.py         # MatchingEngine (waterfall blocking, salted joins)
│   │   └── resolution_manager.py # Logs match decisions to CSV
│   ├── governance/
│   │   └── survivorship.py     # SurvivorshipEngine (trust score + recency → Golden Record)
│   ├── metadata/
│   │   └── registry.py         # SemanticType definitions + regex validation
│   ├── auth/user_manager.py    # JSON-backed user store with RBAC
│   ├── audit/logger.py         # CSV-backed audit log
│   └── tools/
│       ├── secret_manager.py   # Databricks secret scope helper
│       └── generate_synthetic_data.py
config/
├── system_config.yaml          # Spark settings, storage paths, governance thresholds
└── semantic_types.yaml         # PII regex definitions (NPI, SSN, Email, Phone, etc.)
assets/                         # Logos for data sources and sidebar icons
data/                           # Local fallback data, audit logs, user store
```

## Key Patterns

| Pattern | Where | Detail |
|---------|-------|--------|
| Adapter / Strategy | `connectors/adapters/` | Subclass `BaseConnectorAdapter` for each DB |
| Singleton | `ConnectorService`, `Bootstrapper` | Module-level `_instance` + `get_*()` factory |
| Medallion Architecture | `config/system_config.yaml` | Bronze (raw) → Silver (clean) → Gold (resolved) |
| RBAC | `app.py` → `PAGE_PERMISSIONS` | Roles: Admin, Executive, Steward, Developer |
| CSS-in-Python | `app.py`, all views | Inject via `st.markdown("<style>...</style>", unsafe_allow_html=True)` |

## Code Conventions
- Every frontend view **must** export a `render()` function — `app.py` calls it for routing.
- Backend services are **singletons** — use `get_connector_service()`, `get_bootstrapper()`.
- Session state keys: use `st.session_state["key"]` — check `app.py` for initialized keys.
- The `configuration_metadata` Databricks table stores connector + pipeline configs as JSON columns.
- Brand color: **#D11F41** (ruby red). Font: **Inter** (Google Fonts). Dark bg: **#0f172a**.

## Key Files by Size (touch with care)
| File | Lines | Why it's large |
|------|-------|----------------|
| `inspector.py` | 3115 | Full pipeline UI with 5 stages + CSS |
| `connector_service.py` | 1667 | All connector orchestration + Databricks queries |
| `app.py` | 1611 | Global CSS + auth + routing |
| `connectors.py` | 1271 | Connection wizard + schema browser |
| `match_review.py` | 958 | Record comparison cards + CSS |

## Deeper Context (read on-demand, not upfront)
- [Architecture & Backend Layers](docs/architecture.md)
- [Data Pipeline Flow](docs/data-flow.md)
- [Styling & CSS Guide](docs/styling-guide.md)

## Workflows (step-by-step recipes)
- [Add a New Connector](.agents/workflows/add-connector.md)
- [Add a Frontend View](.agents/workflows/add-frontend-view.md)
- [Run Locally](.agents/workflows/run-locally.md)
