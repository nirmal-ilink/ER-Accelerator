import os
import streamlit as st
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel

class SecretManager:
    """
    Manages Databricks Secret Scopes and Secrets.
    Uses Databricks SDK for management operations.
    """
    
    def __init__(self, host: Optional[str] = None, token: Optional[str] = None):
        # Resolve credentials from Streamlit secrets if not provided
        if not host or not token:
            try:
                if hasattr(st, "secrets"):
                    host = host or st.secrets.get("DATABRICKS_HOST")
                    token = token or st.secrets.get("DATABRICKS_TOKEN")
            except Exception:
                pass

        # If we still don't have them, the SDK will check env vars or config
        self.w = WorkspaceClient(host=host, token=token)
        self.scope_name = "mdm-connectors"

    def ensure_scope(self):
        """Ensures the secret scope exists, creates it if not."""
        try:
            # Check if scope exists
            scopes = list(self.w.secrets.list_scopes())
            scope_names = [s.name for s in scopes]
            
            if self.scope_name not in scope_names:
                print(f"Creating secret scope: {self.scope_name}")
                self.w.secrets.create_scope(scope=self.scope_name)
            else:
                print(f"Secret scope already exists: {self.scope_name}")
                
        except Exception as e:
            # If we fail to list or create, it might be a permission issue
            print(f"Error ensuring scope: {str(e)}")
            raise

    def put_secret(self, key: str, value: str):
        """Saves a secret to the scope."""
        try:
            self.ensure_scope()
            self.w.secrets.put_secret(scope=self.scope_name, key=key, string_value=value)
            return True
        except Exception as e:
            print(f"Error putting secret: {str(e)}")
            raise

    def get_secret_metadata_pointer(self, key: str) -> str:
        """Returns the pointer string to be stored in metadata tables."""
        return f"databricks://secrets/{self.scope_name}/{key}"

def get_secret_manager(host: Optional[str] = None, token: Optional[str] = None) -> SecretManager:
    return SecretManager(host=host, token=token)
