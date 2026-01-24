import yaml
import os
import streamlit as st
from typing import Dict, Any, Optional

class Bootstrapper:
    """
    Initializes the Platform Environment.
    - Loads System Configuration
    - Initializes Spark Session with Databricks Connect or Local Spark
    
    Databricks Connect Integration:
    When DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_CLUSTER_ID are configured
    (via secrets.toml or environment variables), this will connect to the remote
    Databricks cluster for all Spark operations.
    """
    
    def __init__(self, config_class_path: str = None):
        # Resolve config path relative to this script's location if not provided or if relative
        if config_class_path is None:
            # Default to ../../config/system_config.yaml relative to src/backend/bootstrapper.py
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.config_path = os.path.join(base_dir, "config", "system_config.yaml")
        else:
            self.config_path = config_class_path

        self.config: Dict[str, Any] = self._load_config()
        self.spark = self._init_spark()
        self._is_databricks_connected: bool = False

    def _load_config(self) -> Dict[str, Any]:
        """Loads system configuration from YAML."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"System Configuration not found at {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def _get_databricks_credentials(self) -> Optional[Dict[str, str]]:
        """
        Retrieves Databricks credentials from Streamlit secrets or environment variables.
        Returns None if credentials are not fully configured.
        """
        host = None
        token = None
        cluster_id = None
        
        # Priority 1: Streamlit secrets (for Streamlit apps)
        try:
            if hasattr(st, 'secrets'):
                host = st.secrets.get("DATABRICKS_HOST")
                token = st.secrets.get("DATABRICKS_TOKEN")
                cluster_id = st.secrets.get("DATABRICKS_CLUSTER_ID")
        except Exception:
            pass  # Streamlit secrets not available (e.g., running outside Streamlit)
        
        # Priority 2: Environment variables (for CLI/notebooks or Databricks Apps)
        if not host:
            host = os.environ.get("DATABRICKS_HOST")
        if not token:
            token = os.environ.get("DATABRICKS_TOKEN")
        if not cluster_id:
            cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
        
        # Validate credentials are present and not placeholders
        if host and token and cluster_id:
            # Check for placeholder values
            if "YOUR_" in host or "YOUR_" in cluster_id:
                print("WARNING: Databricks credentials contain placeholder values. Falling back to local Spark.")
                return None
            return {
                "host": host,
                "token": token,
                "cluster_id": cluster_id
            }
        
        return None

    def _init_spark(self):
        """
        Initializes Spark Session.
        
        Priority:
        1. Databricks Runtime (already running in Databricks)
        2. Databricks Connect (remote execution on Databricks cluster)
        3. Local Spark (fallback for development)
        """
        app_name = self.config['system']['spark']['app_name']
        master = self.config['system']['spark'].get('master', 'local[*]')
        
        # Check if running in Databricks Runtime (notebook or job)
        is_databricks_runtime = "DATABRICKS_RUNTIME_VERSION" in os.environ
        
        if is_databricks_runtime:
            print(f"INFO: Running in Databricks Runtime. Using existing Spark session.")
            self._is_databricks_connected = True
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            return spark
        
        # Try Databricks Connect
        db_creds = self._get_databricks_credentials()
        if db_creds:
            try:
                from databricks.connect import DatabricksSession
                
                print(f"INFO: Initializing Databricks Connect session '{app_name}'...")
                print(f"INFO: Connecting to: {db_creds['host']}")
                print(f"INFO: Cluster ID: {db_creds['cluster_id']}")
                
                spark = DatabricksSession.builder \
                    .remote(
                        host=db_creds['host'],
                        token=db_creds['token'],
                        cluster_id=db_creds['cluster_id']
                    ) \
                    .getOrCreate()
                
                self._is_databricks_connected = True
                print("INFO: Successfully connected to Databricks cluster!")
                return spark
                
            except ImportError:
                print("WARNING: databricks-connect not installed. Run: pip install databricks-connect")
                print("WARNING: Falling back to local Spark session.")
            except Exception as e:
                print(f"ERROR: Failed to connect to Databricks: {e}")
                print("WARNING: Falling back to local Spark session.")
        
        # Fallback: Local Spark
        print(f"INFO: Initializing local Spark Session '{app_name}'...")
        print(f"INFO: Setting local master: {master}")
        
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .getOrCreate()
        
        self._is_databricks_connected = False
        return spark

    def is_connected_to_databricks(self) -> bool:
        """Returns True if connected to Databricks (either Runtime or Connect)."""
        return self._is_databricks_connected

    def get_storage_path(self, zone: str) -> str:
        """
        Resolves storage path based on environment.
        Returns Unity Catalog/DBFS path for Databricks, or Local path for testing.
        """
        if self._is_databricks_connected:
            # Use Databricks paths (Unity Catalog Volumes or DBFS)
            return self.config['paths'].get(zone, f"/tmp/{zone}/")
        else:
            # Fallback for Local Air-Gap Testing
            if zone == "landing" and "local_landing" in self.config['paths']:
                return self.config['paths']["local_landing"]

            if zone in self.config['paths']:
                return self.config['paths'][zone]
            
            # Map logical zones to local structure
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
            data_root = os.path.join(project_root, "data")
            
            if zone == "system": return os.path.join(data_root, "system", "")
            return os.path.join(data_root, zone, "")


# Factory method for quick access
def get_bootstrapper(path: str = None) -> Bootstrapper:
    """
    Returns a Bootstrapper instance.
    
    The Bootstrapper will automatically connect to Databricks if credentials
    are configured in secrets.toml or environment variables.
    """
    return Bootstrapper(path)
