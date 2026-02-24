---
description: How to run the ER-Accelerator locally
---

# Run Locally

## Prerequisites
- Python 3.12
- A Databricks workspace (optional — app falls back to local Spark)

## Steps

// turbo-all

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure Databricks (optional) — edit `.streamlit/secrets.toml`:
   ```toml
   DATABRICKS_HOST = "https://adb-xxxxx.azuredatabricks.net"
   DATABRICKS_CLUSTER_ID = "your-cluster-id"
   DATABRICKS_TOKEN = "your-token"
   ```

3. Start the app:
   ```
   streamlit run src/frontend/app.py
   ```

4. Login with default credentials:
   - `admin` / `admin123` (Admin — full access)
   - `dev` / `dev123` (Developer)
   - `steward` / `steward123` (Data Steward)
   - `exec` / `exec123` (Executive — dashboard only)
