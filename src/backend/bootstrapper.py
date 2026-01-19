import yaml
import os
from pyspark.sql import SparkSession
from typing import Dict, Any

class Bootstrapper:
    """
    Initializes the Platform Environment.
    - Loads System Configuration
    - Initializes Spark Session with Enterprise Settings (AQE)
    """
    
    def __init__(self, config_class_path: str = "config/system_config.yaml"):
        self.config_path = config_class_path
        self.config: Dict[str, Any] = self._load_config()
        self.spark: SparkSession = self._init_spark()

    def _load_config(self) -> Dict[str, Any]:
        """Loads system configuration from YAML."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"System Configuration not found at {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def _init_spark(self) -> SparkSession:
        """
        Initializes Spark Session with Adaptive Query Execution (AQE) enabled.
        Configures for Databricks CE or Local mode based on config.
        """
        app_name = self.config['system']['spark']['app_name']
        master = self.config['system']['spark'].get('master', 'local[*]')
        
        print(f"INFO: Initializing Spark Session '{app_name}' on master '{master}'...")
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # In a real Databricks Runtime, Spark is already active.
        # This check prevents re-initialization errors in notebooks.
        try:
            spark = SparkSession.builder.getOrCreate()
            # Apply runtime configs if possible, though mostly set at startup
            for key, val in builder._options.items():
                spark.conf.set(key, val)
            return spark
        except Exception:
            return builder.getOrCreate()

    def get_storage_path(self, zone: str) -> str:
        """
        Resolves storage path based on environment.
        Returns DBFS path for CE, or Local path for testing.
        """
        # Logic to detect if running on Databricks Community Edition
        is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        
        if is_databricks:
            return self.config['paths'].get(zone, f"/tmp/{zone}/")
        else:
            # Fallback for Local Air-Gap Testing
            # If the zone is explicitly defined in paths (e.g. 'local_landing'), use it
            if zone in self.config['paths']:
                return self.config['paths'][zone]
            
            # Otherwise map logical zones to local structure
            base_dir = self.config['paths'].get('local_landing', './data/')
            # If base_dir is ./data/landing/, we might want just ./data/ for zones like 'system'
            # Adjusting to standard local structure: data/bronze, data/system
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
            data_root = os.path.join(project_root, "data")
            
            if zone == "system": return os.path.join(data_root, "system", "")
            return os.path.join(data_root, zone, "")

# Factory method for quick access
def get_bootstrapper(path: str = "config/system_config.yaml") -> Bootstrapper:
    return Bootstrapper(path)
