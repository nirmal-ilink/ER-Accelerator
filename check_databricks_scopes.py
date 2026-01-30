from databricks.sdk import WorkspaceClient
import streamlit as st
import os
import tomllib

# Mocking st.secrets if not running in streamlit
if not hasattr(st, "secrets") or not st.secrets:
    try:
        secrets_path = os.path.join(".streamlit", "secrets.toml")
        with open(secrets_path, "rb") as f:
            st.secrets = tomllib.load(f)
    except Exception as e:
        print(f"Warning: Could not load secrets from .streamlit/secrets.toml: {e}")

# Uses your existing secrets.toml
try:
    w = WorkspaceClient(
        host=st.secrets["DATABRICKS_HOST"],
        token=st.secrets["DATABRICKS_TOKEN"]
    )

    print("Fetching secret scopes...")
    scopes = w.secrets.list_scopes()
    names = [s.name for s in scopes]
    print(f"Found {len(names)} scopes: {names}")
    
    if "mdm-connectors" in names:
        print("Success: 'mdm-connectors' is ready.")
    else:
        print("Missing: 'mdm-connectors' not found.")
        
    # Also check for the one we just decided on
    if "er_connector_secrets" in names:
        print("Info: 'er_connector_secrets' is also ready.")
        
except Exception as e:
    print(f"Error: {e}")
