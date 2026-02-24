---
description: How to add a new data source connector (e.g., Snowflake, Oracle)
---

# Add a New Connector

## Steps

1. **Create the adapter** at `src/backend/connectors/adapters/<name>_adapter.py`
   - Subclass `BaseConnectorAdapter` from `base_adapter.py`
   - Implement all abstract methods: `connector_type`, `display_name`, `required_fields`, `build_jdbc_url`, `get_jdbc_properties`, `get_jdbc_driver_class`, `fetch_schemas_and_tables`, `test_connection`, `fetch_columns`
   - See `sql_server_adapter.py` or `databricks_adapter.py` for reference

2. **Export the adapter** in `src/backend/connectors/adapters/__init__.py`
   ```python
   from .new_adapter import NewAdapter
   ```

3. **Register in ConnectorService** — edit `src/backend/connectors/connector_service.py`
   - In `_register_adapters()`, add: `self._adapters[adapter.connector_type] = adapter`

4. **Add the logo** — place a PNG in `assets/<name>_logo.png`

5. **Add logo CSS** — in `src/frontend/views/connectors.py`, add a CSS rule inside the logo injection block:
   ```css
   li[role="option"]:has([data-testid*="NewSource"]) { background-image: url(data:image/png;base64,...); }
   ```
   Use `get_img_as_base64()` helper to encode the logo.

6. **Test** — run `streamlit run src/frontend/app.py`, go to Connectors page, verify new source appears in dropdown with logo, test connection works.
