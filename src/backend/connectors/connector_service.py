"""
Connector Service - Core orchestration layer for all data source connectors.

This service provides a unified interface for:
- Testing connections
- Fetching metadata (schemas/tables)
- Saving and loading connector configurations
- Managing credentials securely via Databricks Secrets

Usage:
    from src.backend.connectors import get_connector_service
    
    service = get_connector_service()
    
    # Test connection
    result = service.test_connection("sqlserver", config)
    
    # Fetch schemas and tables
    metadata = service.fetch_metadata("sqlserver", config)
    
    # Save configuration with selected tables
    service.save_configuration("sqlserver", config, selected_tables)
"""

import json
import os
import datetime # Added for cache timestamp
import uuid  # Added for unique IDs
import threading # Added for async save
import streamlit as st
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
import functools
import time

# Constants
CACHE_FILE_NAME = "connector_cache.json"
TRACE_LOG = "trace.log"

def _log_trace(msg):
    try:
        with open(TRACE_LOG, "a") as f:
            f.write(f"[{datetime.datetime.now()}] {msg}\n")
    except:
        pass

# Module-level singleton instance
_service_instance = None

# Import Audit Logger
from src.backend.audit.logger import AuditLogger

# Import adapters
from .adapters import SQLServerAdapter, DatabricksAdapter
from .adapters.base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata

def _with_retry(func):
    """Decorator to retry methods on Spark session loss."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            # Check for common Spark session loss errors
            # 1. [NO_ACTIVE_SESSION] - Databricks Connect session expired
            # 2. INVALID_HANDLE_VALUE - Windows socket issue with Databricks Connect
            # 3. Connection refused - Spark/Java process died
            if any(err in error_msg for err in ["[NO_ACTIVE_SESSION]", "INVALID_HANDLE_VALUE", "Connection refused"]):
                print(f"WARNING: Detected Spark session loss in {func.__name__}: {error_msg}")
                
                # Attempt recovery if method exists
                if hasattr(self, '_retry_recovery'):
                    try:
                        self._retry_recovery()
                        print(f"INFO: Retrying {func.__name__} after recovery...")
                        return func(self, *args, **kwargs)
                    except Exception as recovery_error:
                        print(f"ERROR: Retry failed: {recovery_error}")
                        raise e  # Raise original error if recovery fails
                else:
                    print("WARNING: _retry_recovery method not found on object.")
            
            raise e
    return wrapper


@dataclass
class ConnectorConfig:
    """Stored connector configuration (maps to configuration_metadata table)."""
    connection_id: str  # Unique ID
    connector_type: str
    connector_name: str
    connection_name: str  # User-given name (unique per user)
    config: Dict[str, Any]  # source_configuration column (JSON)
    selected_tables: Dict[str, Any]  # tbl_configuration column (JSON) — per-schema table list with load_type/watermark
    status: str  # 'active', 'inactive', 'error'
    created_by: str = "System"  # Username who created this connection
    last_sync_time: Optional[str] = None  # ISO timestamp from updated_at
    # Scheduling
    schedule_enabled: bool = False
    schedule_cron: Optional[str] = None  # e.g., '0 2 * * *'
    schedule_timezone: str = "UTC"
    # Extended configuration (nullable, populated later)
    bronze_configuration: Optional[str] = None
    silver_configuration: Optional[str] = None


class ConnectorService:
    """
    Central service for managing data source connectors.
    
    Provides:
    - Adapter registry for different database types
    - Unified API for connection testing and metadata discovery
    - Configuration persistence to Databricks Delta tables
    - Secure credential management via Databricks Secrets
    """
    
    # Registry of available adapters
    _adapters: Dict[str, BaseConnectorAdapter] = {}
    
    def __init__(self, spark=None):
        """
        Initialize the connector service.
        
        Args:
            spark: Optional SparkSession. If not provided, will be initialized
                   via Bootstrapper when needed.
        """
        self._spark = spark
        self._secret_manager = None
        self.audit_logger = AuditLogger()
        self._register_adapters()

    def _retry_recovery(self):
        """Attempts to recover a lost Spark session."""
        print("WARNING: Attempting to recover Spark session...")
        try:
            from src.backend.bootstrapper import get_bootstrapper
            # Resetting via bootstrapper returns the new session
            self._spark = get_bootstrapper().reset_spark()
            print("INFO: Spark session recovered successfully.")
        except Exception as e:
            print(f"ERROR: Failed to recover Spark session: {e}")
            raise e

    def _get_cache_path(self):
        """Returns key for local cache file."""
        # Use a specific directory for cache, or current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, CACHE_FILE_NAME)

    def _save_to_cache(self, config_data: Dict[str, Any]):
        """Saves configuration to local JSON cache."""
        try:
            cache_path = self._get_cache_path()
            with open(cache_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            print(f"INFO: Saved configuration to local cache: {cache_path}")
        except Exception as e:
            print(f"WARNING: Failed to write to local cache: {e}")

    def _read_from_cache(self) -> Optional[Dict[str, Any]]:
        """Reads configuration from local JSON cache."""
        try:
            cache_path = self._get_cache_path()
            if os.path.exists(cache_path):
                with open(cache_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"WARNING: Failed to read from local cache: {e}")
        return None
    
    def _register_adapters(self):
        """Register all available connector adapters."""
        import importlib
        
        try:
            # Import modules directly to allow reloading
            from .adapters import sql_server_adapter, databricks_adapter
            
            # Reload modules to pick up changes (hot-fix support)
            importlib.reload(sql_server_adapter)
            importlib.reload(databricks_adapter)
            
            # SQL Server
            sql_adapter = sql_server_adapter.SQLServerAdapter()
            self._adapters[sql_adapter.connector_type] = sql_adapter
            
            # Databricks / Unity Catalog
            databricks_adapter = databricks_adapter.DatabricksAdapter()
            self._adapters[databricks_adapter.connector_type] = databricks_adapter
            
        except Exception as e:
            print(f"WARNING: Failed to hot-reload adapters: {e}")
            # Fallback to standard registration if reload fails
            from .adapters import SQLServerAdapter, DatabricksAdapter
            
            sql_adapter = SQLServerAdapter()
            self._adapters[sql_adapter.connector_type] = sql_adapter
            
            databricks_adapter = DatabricksAdapter()
            self._adapters[databricks_adapter.connector_type] = databricks_adapter
        
        # Future: Add more adapters here
        # self._adapters['snowflake'] = SnowflakeAdapter()
        # self._adapters['oracle'] = OracleAdapter()
    
    @property
    def spark(self):
        """Lazy initialization of Spark session."""
        if self._spark is None:
            # Import here to avoid circular imports
            from src.backend.bootstrapper import get_bootstrapper
            self._spark = get_bootstrapper().spark
        return self._spark
    
    @property
    def secret_manager(self):
        """Lazy initialization of secret manager."""
        if self._secret_manager is None:
            from src.backend.tools.secret_manager import get_secret_manager
            self._secret_manager = get_secret_manager()
        return self._secret_manager
    
    def get_adapter(self, connector_type: str) -> BaseConnectorAdapter:
        """
        Get the adapter for a specific connector type.
        
        Args:
            connector_type: Type identifier (e.g., 'sqlserver')
            
        Returns:
            The appropriate adapter instance
            
        Raises:
            ValueError: If connector type is not supported
        """
        connector_type = connector_type.lower()
        if connector_type not in self._adapters:
            supported = ", ".join(self._adapters.keys())
            raise ValueError(
                f"Unsupported connector type: '{connector_type}'. "
                f"Supported types: {supported}"
            )
        return self._adapters[connector_type]
    
    def get_available_connectors(self) -> List[Dict[str, str]]:
        """
        Returns list of available connector types with metadata.
        
        Returns:
            List of dicts with 'type', 'name', 'fields'
        """
        return [
            {
                "type": adapter.connector_type,
                "name": adapter.display_name,
                "fields": adapter.required_fields
            }
            for adapter in self._adapters.values()
        ]

    @_with_retry
    def test_connection(self, connector_type: str, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Test connection using the appropriate adapter.
        """
        # Resolve secrets before testing
        resolved_config = self._resolve_secrets(config)
        adapter = self.get_adapter(connector_type)
        
        # If the adapter object is stale (from before code reload), force one-time re-registration
        if not hasattr(adapter, 'requires_spark_for_test'):
            self._adapters = {} # Clear cache
            self._register_adapters() # Re-instantiate
            adapter = self.get_adapter(connector_type)
        
        # Only initialize Spark if the adapter requires it for testing
        requires_spark = getattr(adapter, 'requires_spark_for_test', True)
        spark_session = self.spark if requires_spark else None
        
        # Wrapper for timeout protection
        import threading
        result_container = {"result": None, "error": None}
        
        def _run_test():
            try:
                result_container["result"] = adapter.test_connection(spark_session, resolved_config)
            except Exception as e:
                result_container["error"] = e
        
        # use a 20s timeout (adapter internal timeout is usually 10s, this is a safety net)
        thread = threading.Thread(target=_run_test)
        thread.daemon = True
        thread.start()
        thread.join(timeout=20)
        
        if thread.is_alive():
            return ConnectionTestResult(
                success=False,
                message="Connection test timed out. The server is unreachable or the cluster is unresponsive."
            )
            
        if result_container["error"]:
            raise result_container["error"]
            
        return result_container["result"]

    def fetch_metadata(self, connector_type: str, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Fetch schema metadata using the appropriate adapter.
        """
        # Resolve secrets before fetching
        resolved_config = self._resolve_secrets(config)
        adapter = self.get_adapter(connector_type)
        return adapter.fetch_schemas_and_tables(self.spark, resolved_config)

    @_with_retry
    def fetch_catalogs(self, connector_type: str, config: Dict[str, Any]) -> List[str]:
        """
        Fetch available catalogs from a data source (Databricks only).
        
        Args:
            connector_type: Type of connector (e.g., 'databricks')
            config: Connection configuration dictionary
            
        Returns:
            List of catalog names
        """
        adapter = self.get_adapter(connector_type)
        
        # Hot-reload support: if adapter instance is stale (missing new method), re-register
        if not hasattr(adapter, 'fetch_catalogs'):
            print(f"DEBUG: Adapter {connector_type} missing fetch_catalogs. Reloading adapters...")
            self._adapters = {}
            self._register_adapters()
            adapter = self.get_adapter(connector_type)

        if hasattr(adapter, 'fetch_catalogs'):
            # Resolve secrets before fetching catalogs
            resolved_config = self._resolve_secrets(config)
            return adapter.fetch_catalogs(self.spark, resolved_config)
        else:
            raise NotImplementedError(f"Connector '{connector_type}' does not support catalog discovery")

    def fetch_columns(
        self, 
        connector_type: str, 
        config: Dict[str, Any],
        schema: str,
        table: str
    ) -> List[Dict[str, str]]:
        """
        Fetch columns for a specific table.
        
        Args:
            connector_type: Type of connector (e.g., 'sqlserver')
            config: Connection configuration dictionary
            schema: Schema name
            table: Table name
            
        Returns:
            List of columns with names and types
        """
        resolved_config = self._resolve_secrets(config)
        adapter = self.get_adapter(connector_type)
        return adapter.fetch_columns(self.spark, resolved_config, schema, table)
    
    def _resolve_secrets(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve secret pointers to actual values.
        
        Secret pointers look like: databricks://secrets/scope/key
        
        Args:
            config: Configuration that may contain secret pointers
            
        Returns:
            Configuration with secrets resolved
        """
        resolved = {}
        
        # Temporary file logger for debugging
        debug_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_auth.log")
        def log_debug(msg):
            try:
                with open(debug_log_path, "a") as f:
                    timestamp = datetime.datetime.now().isoformat()
                    f.write(f"[{timestamp}] {msg}\n")
            except:
                pass

        log_debug(f"Resolving secrets for keys: {list(config.keys())}")
        
        for key, value in config.items():
            if isinstance(value, str) and value.startswith("databricks://secrets/"):
                log_debug(f"Found secret pointer for '{key}': {value}")
                
                # Resolve explicitly using SecretManager because JDBC properties 
                # do not support Spark secret syntax (they are passed to driver)
                secret_val = self.secret_manager.get_secret(value)
                
                if secret_val:
                    # CRITICAL: Strip whitespace/newlines that Databricks Secrets
                    # may include (common cause of JDBC Login failures)
                    stripped_val = secret_val.strip()
                    if len(stripped_val) != len(secret_val):
                        log_debug(f"STRIPPED secret for '{key}': original_len={len(secret_val)}, stripped_len={len(stripped_val)}")
                    resolved[key] = stripped_val
                    log_debug(f"Resolved secret for '{key}' (Length: {len(stripped_val)})")
                else:
                    log_debug(f"WARNING: Failed to resolve secret for '{key}'")
                    print(f"WARNING: Failed to resolve secret pointer: {value}")
                    resolved[key] = value
            else:
                resolved[key] = value
        return resolved
    
    def _store_secrets(
        self, 
        connector_type: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Store sensitive fields in Databricks Secrets and return config with pointers.
        """
        processed_config = {}
        adapter = self.get_adapter(connector_type)
        
        # Simple heuristic for secret fields
        secret_keywords = ['password', 'token', 'key', 'secret']
        
        for key, value in config.items():
            is_secret = any(keyword in key.lower() for keyword in secret_keywords)
            
            if is_secret and value and not value.startswith("databricks://"):
                # Store in Databricks Secrets
                secret_key = f"{connector_type}_{key.lower().replace(' ', '_')}"
                try:
                    self.secret_manager.put_secret(secret_key, value)
                    # Replace with pointer
                    processed_config[key] = self.secret_manager.get_secret_metadata_pointer(secret_key)
                except Exception as e:
                    print(f"WARNING: Could not store secret '{key}': {e}")
                    # Keep raw value as fallback (not ideal but prevents data loss)
                    processed_config[key] = value
            else:
                processed_config[key] = value
        
        return processed_config
    
    @_with_retry
    def save_configuration(
        self,
        connector_type: str,
        connector_name: str,
        config: Dict[str, Any],
        connection_name: str = "",
        created_by: str = "System",
        selected_tables: Optional[Dict[str, Any]] = None,
        status: str = "active",
        schedule_enabled: bool = False,
        schedule_cron: Optional[str] = None,
        schedule_timezone: str = "UTC"
    ) -> str:
        """
        Save connector configuration to configuration_metadata table.
        
        Steps:
        1. Stores secrets (fast)
        2. Saves to local cache (fast)
        3. Writes to Delta Table (synchronous — ensures data is persisted)
        
        Returns connection_id after successful persistence.
        """
        if selected_tables is None:
            selected_tables = {}
        
        # Store secrets and get config with pointers
        safe_config = self._store_secrets(connector_type, config)
        
        # Generate unique connection ID using UUID
        connection_id = str(uuid.uuid4())
        source_type = connector_type
        
        # Filter out meta fields from source_configuration json
        meta_fields = ['selected_tables', 'schedule_enabled', 'schedule_cron', 'schedule_timezone']
        source_configuration = {k: v for k, v in safe_config.items() if k not in meta_fields and k != 'selected_tables'}
        
        # JSON serialization
        source_configuration_json = json.dumps(source_configuration).replace("'", "''")
        tbl_configuration_json = json.dumps(selected_tables).replace("'", "''")
        
        # Update Local Cache - IMMEDIATE UI FEEDBACK
        cache_data = {
            "connection_id": connection_id,
            "source_type": source_type,
            "source_name": connector_name,
            "connection_name": connection_name,
            "created_by": created_by,
            "source_configuration": source_configuration_json,
            "tbl_configuration": tbl_configuration_json,
            "schedule_enabled": schedule_enabled,
            "schedule_cron": schedule_cron,
            "schedule_timezone": schedule_timezone,
            "status": status,
            "updated_at": datetime.datetime.now().isoformat()
        }
        self._save_to_cache(cache_data)
        
        # Persist to Delta Table SYNCHRONOUSLY
        self._persist_to_delta_sync(
            connection_id=connection_id,
            connector_type=connector_type,
            connector_name=connector_name,
            connection_name=connection_name,
            created_by=created_by,
            source_configuration_json=source_configuration_json,
            tbl_configuration_json=tbl_configuration_json,
            schedule_enabled=schedule_enabled,
            schedule_cron=schedule_cron,
            schedule_timezone=schedule_timezone,
            status=status,
            user=created_by
        )
        
        return connection_id

    def _persist_to_delta_sync(
        self,
        connection_id: str,
        connector_type: str,
        connector_name: str,
        connection_name: str,
        created_by: str,
        source_configuration_json: str,
        tbl_configuration_json: str,
        schedule_enabled: bool,
        schedule_cron: Optional[str],
        schedule_timezone: str,
        status: str,
        user: str
    ):
        """
        Write configuration to Delta table (configuration_metadata) synchronously.
        Raises on failure so the caller can surface errors to the user.
        """
        print(f"INFO: [Save] Persisting connection {connection_id} to Delta table...")
        
        # Read catalog/schema config in the main thread (st.secrets works here)
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        def escape_sql(v):
            if v is None:
                return "NULL"
            val = str(v).replace("'", "''")
            return f"'{val}'"
        
        # Insert new record
        insert_sql = f"""
            INSERT INTO {target_table} (
                connection_id, connection_name, source_type, source_name,
                created_by, source_configuration, tbl_configuration,
                schedule_enabled, schedule_cron, schedule_timezone,
                status, bronze_configuration, silver_configuration,
                created_at, updated_at
            )
            VALUES (
                '{connection_id}', {escape_sql(connection_name)}, '{connector_type}', {escape_sql(connector_name)},
                {escape_sql(created_by)}, '{source_configuration_json}', '{tbl_configuration_json}',
                {str(schedule_enabled).lower()}, {escape_sql(schedule_cron)}, {escape_sql(schedule_timezone)},
                '{status}', NULL, NULL,
                current_timestamp(), current_timestamp()
            )
        """
        
        try:
            self.spark.sql(insert_sql).collect()
            
            self.audit_logger.log_event(
                user=user,
                action="Saved Configuration",
                module="Connectors",
                status="Success",
                details=f"Saved {connector_type} configuration '{connector_name}' (ID: {connection_id})"
            )
            print(f"INFO: [Save] Successfully wrote {connection_id} to Delta table.")
            
        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: [Save] Failed to write {connection_id}: {error_msg}")
            
            # CASE 1: Session Death (Databricks Connect timeout)
            if "[NO_ACTIVE_SESSION]" in error_msg:
                print("WARNING: Spark Session is not active. Attempting to re-initialize...")
                
                # Re-initialize Spark Session
                try:
                    from src.backend.bootstrapper import get_bootstrapper
                    self._spark = get_bootstrapper().reset_spark()
                    
                    self.spark.sql(insert_sql).collect()
                    
                    self.audit_logger.log_event(
                        user=user,
                        action="Saved Configuration",
                        module="Connectors",
                        status="Success",
                        details=f"Saved {connector_type} configuration '{connector_name}' (ID: {connection_id}) after session recovery"
                    )
                    print(f"INFO: [Save] Recovered and wrote {connection_id}.")
                except Exception as retry_e:
                    print(f"ERROR: [Save] Recovery failed for {connection_id}: {retry_e}")
                    raise retry_e
            
            else:
                # Log failure and re-raise so UI can show the error
                self.audit_logger.log_event(
                    user=user,
                    action="Saved Configuration",
                    module="Connectors",
                    status="Failed",
                    details=f"Failed to save {connector_type} configuration: {error_msg}"
                )
                raise
    
    def load_configuration(
        self, 
        connector_type: str
    ) -> Optional[ConnectorConfig]:
        """
        Load latest saved configuration for a connector type.
        
        Args:
            connector_type: Type of connector to load
            
        Returns:
            ConnectorConfig if found, None otherwise
        """
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        try:
            try:
                self.spark.sql(f"REFRESH TABLE {target_table}")
            except Exception as e:
                print(f"WARNING: REFRESH TABLE failed: {e}")

            # Query the latest configuration for this source_type
            df = self.spark.sql(f"SELECT * FROM {target_table} WHERE source_type = '{connector_type}' ORDER BY updated_at DESC LIMIT 1")
            
            rows = df.collect()
            if not rows:
                return None
            
            row = rows[0]
            row_dict = row.asDict()
            
            # Helper to safely get value from row
            def safe_get(row_dict, key, default=None):
                return row_dict.get(key, default)

            # Parse JSON fields (new column names)
            config_dict = json.loads(safe_get(row_dict, 'source_configuration', '{}'))
            try:
                selected_tables = json.loads(safe_get(row_dict, 'tbl_configuration', '{}'))
            except:
                selected_tables = {}
            
            updated_at = safe_get(row_dict, 'updated_at')
            last_sync = updated_at.isoformat() if updated_at else None

            return ConnectorConfig(
                connection_id=safe_get(row_dict, 'connection_id'),
                connector_type=safe_get(row_dict, 'source_type'),
                connector_name=safe_get(row_dict, 'source_name'),
                connection_name=safe_get(row_dict, 'connection_name', ""),
                config=config_dict,
                selected_tables=selected_tables,
                status=safe_get(row_dict, 'status', 'inactive'),
                created_by=safe_get(row_dict, 'created_by', 'System'),
                last_sync_time=last_sync,
                schedule_enabled=safe_get(row_dict, 'schedule_enabled', False),
                schedule_cron=safe_get(row_dict, 'schedule_cron'),
                schedule_timezone=safe_get(row_dict, 'schedule_timezone', 'UTC'),
                bronze_configuration=safe_get(row_dict, 'bronze_configuration'),
                silver_configuration=safe_get(row_dict, 'silver_configuration')
            )
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return None
            print(f"Error loading configuration: {e}")
            return None

    def get_latest_configuration(self) -> Optional[ConnectorConfig]:
        """
        Retrieves the most recently saved connector configuration across all types.
        
        This is useful for the Ingestion stage to display the currently active
        data source without needing to know the connector type in advance.
        
        Returns:
            ConnectorConfig of the most recently saved connector, or None.
        """
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        # 1. Try Local Cache First (Fast Path)
        cached_data = self._read_from_cache()
        if cached_data:
            try:
                # Support both old and new cache key names for compatibility
                config_raw = cached_data.get('source_configuration') or cached_data.get('configuration', '{}')
                config_dict = json.loads(config_raw) if isinstance(config_raw, str) else config_raw
                
                selected_tables = {}
                tbl_raw = cached_data.get('tbl_configuration') or cached_data.get('selected_tables')
                if tbl_raw:
                     selected_tables = json.loads(tbl_raw) if isinstance(tbl_raw, str) else tbl_raw

                print("INFO: Loaded connector configuration from local cache.")
                return ConnectorConfig(
                    connection_id=cached_data.get('connection_id'),
                    connector_type=cached_data['source_type'],
                    connector_name=cached_data['source_name'],
                    connection_name=cached_data.get('connection_name', ''),
                    config=config_dict,
                    selected_tables=selected_tables,
                    status=cached_data.get('status', 'active'),
                    created_by=cached_data.get('created_by', 'System'),
                    last_sync_time=cached_data.get('updated_at'),
                    schedule_enabled=cached_data.get('schedule_enabled', False),
                    schedule_cron=cached_data.get('schedule_cron'),
                    schedule_timezone=cached_data.get('schedule_timezone', 'UTC')
                )
            except Exception as e:
                print(f"WARNING: Cache corrupted or invalid format, falling back to Spark: {e}")
                # Fall through to Spark

        try:
            # Query across ALL connector types, ordered by most recent
            df = self.spark.sql(f"""
                SELECT 
                    connection_id, connection_name, source_type, source_name,
                    created_by, source_configuration, tbl_configuration,
                    schedule_enabled, schedule_cron, schedule_timezone,
                    status, bronze_configuration, silver_configuration,
                    updated_at
                FROM {target_table}
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            
            rows = df.collect()
            if not rows:
                return None
            
            row = rows[0]
            
            # Parse JSON fields
            config_dict = json.loads(row['source_configuration'] or '{}')
            try:
                selected_tables = json.loads(row['tbl_configuration'] or '{}')
            except:
                selected_tables = {}
            
            return ConnectorConfig(
                connection_id=row['connection_id'],
                connector_type=row['source_type'],
                connector_name=row['source_name'],
                connection_name=row['connection_name'] or '',
                config=config_dict,
                selected_tables=selected_tables,
                status=row['status'] or 'inactive',
                created_by=row['created_by'] or 'System',
                last_sync_time=row['updated_at'].isoformat() if row['updated_at'] else None,
                schedule_enabled=row['schedule_enabled'],
                schedule_cron=row['schedule_cron'],
                schedule_timezone=row['schedule_timezone'],
                bronze_configuration=row['bronze_configuration'],
                silver_configuration=row['silver_configuration']
            )
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return None
            print(f"Error loading latest configuration: {e}")
            return None

    @_with_retry
    def get_configuration_by_id(self, connection_id: str) -> Optional[ConnectorConfig]:
        """
        Retrieves a connector configuration by its unique connection_id.
        
        This is useful for the Pipeline Inspector when a user pastes
        a specific connection ID to load that particular configuration.
        
        Args:
            connection_id: The UUID of the connection to retrieve
            
        Returns:
            ConnectorConfig if found, None otherwise.
        """
        if not connection_id or not connection_id.strip():
            print("DEBUG: get_configuration_by_id - Empty connection_id provided")
            return None
            
        _log_trace(f"Entering get_configuration_by_id for {connection_id}")
        
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        # Sanitize the connection_id to prevent SQL injection
        safe_id = connection_id.strip().replace("'", "''")
        
        print(f"DEBUG: Looking for connection_id='{safe_id}' in {target_table}")
        
        try:
            # Refresh table metadata to ensure cache is fresh
            try:
                self.spark.sql(f"REFRESH TABLE {target_table}")
            except Exception as e:
                print(f"WARNING: REFRESH TABLE failed: {e}")

            df = self.spark.sql(f"SELECT * FROM {target_table} WHERE connection_id = '{safe_id}' LIMIT 1")
            
            rows = df.collect()
            if not rows:
                print(f"DEBUG: No rows found for connection_id='{safe_id}'")
                
                # DEEP DEBUG: Check what IS in the table
                try:
                    debug_df = self.spark.sql(f"SELECT connection_id, connection_name FROM {target_table} LIMIT 20")
                    debug_rows = debug_df.collect()
                    print(f"DEBUG: Sample existing IDs in table: {str([r['connection_id'] for r in debug_rows])}")
                except Exception as debug_e:
                    print(f"DEBUG: Failed to list table content: {debug_e}")
                    
                return None
            
            row = rows[0]
            row_dict = row.asDict()

            # Helper to safely get value from row
            def safe_get(row_dict, key, default=None):
                return row_dict.get(key, default)
            
            # Parse JSON fields (new column names)
            config_dict = json.loads(safe_get(row_dict, 'source_configuration', '{}'))
            try:
                selected_tables = json.loads(safe_get(row_dict, 'tbl_configuration', '{}'))
            except:
                selected_tables = {}
            
            updated_at = safe_get(row_dict, 'updated_at')
            last_sync = updated_at.isoformat() if updated_at else None

            return ConnectorConfig(
                connection_id=safe_get(row_dict, 'connection_id'),
                connector_type=safe_get(row_dict, 'source_type'),
                connector_name=safe_get(row_dict, 'source_name'),
                connection_name=safe_get(row_dict, 'connection_name', ""),
                config=config_dict,
                selected_tables=selected_tables,
                status=safe_get(row_dict, 'status', 'inactive'),
                created_by=safe_get(row_dict, 'created_by', 'System'),
                last_sync_time=last_sync,
                schedule_enabled=safe_get(row_dict, 'schedule_enabled', False),
                schedule_cron=safe_get(row_dict, 'schedule_cron'),
                schedule_timezone=safe_get(row_dict, 'schedule_timezone', 'UTC'),
                bronze_configuration=safe_get(row_dict, 'bronze_configuration'),
                silver_configuration=safe_get(row_dict, 'silver_configuration')
            )
            
        except Exception as e:
            error_msg = str(e)
            import traceback
            traceback.print_exc()
            print(f"ERROR: get_configuration_by_id failed: {error_msg}")
            
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return None
            return None

    def check_connection_name_exists(self, connection_name: str, created_by: str) -> bool:
        """
        Check if a connection name already exists for the given user.
        
        Args:
            connection_name: The connection name to check
            created_by: The username to scope the check
            
        Returns:
            True if the name already exists for this user, False otherwise.
        """
        if not connection_name or not created_by:
            return False
            
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        safe_name = connection_name.strip().replace("'", "''")
        safe_user = created_by.strip().replace("'", "''")
        
        try:
            # If the column doesn't exist, this query will fail with AnalysisException.
            # In that case, we return False (name doesn't exist/can't be checked).
            df = self.spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM {target_table}
                WHERE LOWER(connection_name) = LOWER('{safe_name}')
                  AND LOWER(created_by) = LOWER('{safe_user}')
            """)
            rows = df.collect()
            return rows[0]['cnt'] > 0 if rows else False
        except Exception:
            return False

    @_with_retry
    def get_user_connections(self, username: str) -> List[ConnectorConfig]:
        """
        Fetch connections created by the specific user.
        """
        safe_user = username.replace("'", "") if username else ""
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        print(f"DEBUG: Querying connections from table: {target_table} for user: {safe_user}")
        
        try:
            # Refresh table metadata to ensure cache is fresh
            try:
                self.spark.sql(f"REFRESH TABLE {target_table}")
            except Exception as e:
                print(f"WARNING: REFRESH TABLE failed: {e}")

            df = self.spark.sql(f"SELECT * FROM {target_table}")
            rows = df.collect()
            
            # Helper to safely get value from row
            def safe_get(row_dict, key, default=None):
                return row_dict.get(key, default)

            connections = []
            for row in rows:
                row_dict = row.asDict()
                
                # Filter by user (case-insensitive)
                created_by = safe_get(row_dict, 'created_by', 'System')
                if str(created_by).lower() != safe_user.lower():
                    continue

                try:
                    config_dict = json.loads(safe_get(row_dict, 'source_configuration', '{}'))
                except:
                    config_dict = {}
                try:
                    selected_tables = json.loads(safe_get(row_dict, 'tbl_configuration', '{}'))
                except:
                    selected_tables = {}
                
                updated_at = safe_get(row_dict, 'updated_at')
                last_sync = updated_at.isoformat() if updated_at else None

                connections.append(ConnectorConfig(
                    connection_id=safe_get(row_dict, 'connection_id'),
                    connector_type=safe_get(row_dict, 'source_type'),
                    connector_name=safe_get(row_dict, 'source_name'),
                    connection_name=safe_get(row_dict, 'connection_name', ""),
                    config=config_dict,
                    selected_tables=selected_tables,
                    status=safe_get(row_dict, 'status', 'inactive'),
                    created_by=created_by,
                    last_sync_time=last_sync,
                    schedule_enabled=safe_get(row_dict, 'schedule_enabled', False),
                    schedule_cron=safe_get(row_dict, 'schedule_cron'),
                    schedule_timezone=safe_get(row_dict, 'schedule_timezone', 'UTC'),
                    bronze_configuration=safe_get(row_dict, 'bronze_configuration'),
                    silver_configuration=safe_get(row_dict, 'silver_configuration')
                ))
            
            # Sort by updated_at desc
            connections.sort(key=lambda x: x.last_sync_time or "", reverse=True)
            return connections
            
        except Exception as e:
            # If table doesn't exist, return empty list
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                return []
            print(f"Error fetching user connections: {e}")
            return []

    def get_all_connections(self) -> List[ConnectorConfig]:
        """
        Fetch all saved connections regardless of user.
        Used as a fallback when user-specific connections are not found.
        """
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        try:
            # Refresh table metadata to ensure cache is fresh
            try:
                self.spark.sql(f"REFRESH TABLE {target_table}")
            except Exception as e:
                print(f"WARNING: REFRESH TABLE failed: {e}")

            df = self.spark.sql(f"SELECT * FROM {target_table}")
            
            rows = df.collect()
            
            # Helper to safely get value from row
            def safe_get(row_dict, key, default=None):
                return row_dict.get(key, default)

            connections = []
            for row in rows:
                try:
                    row_dict = row.asDict()
                    
                    try:
                        config_dict = json.loads(safe_get(row_dict, 'source_configuration', '{}'))
                    except:
                        config_dict = {}
                    try:
                        selected_tables = json.loads(safe_get(row_dict, 'tbl_configuration', '{}'))
                    except:
                        selected_tables = {}
                    
                    # Handle potential missing timestamp
                    updated_at = safe_get(row_dict, 'updated_at')
                    last_sync = updated_at.isoformat() if updated_at else None

                    connections.append(ConnectorConfig(
                        connection_id=safe_get(row_dict, 'connection_id'),
                        connector_type=safe_get(row_dict, 'source_type'),
                        connector_name=safe_get(row_dict, 'source_name'),
                        connection_name=safe_get(row_dict, 'connection_name', ""),
                        config=config_dict,
                        selected_tables=selected_tables,
                        status=safe_get(row_dict, 'status', 'inactive'),
                        created_by=safe_get(row_dict, 'created_by', 'System'),
                        last_sync_time=last_sync,
                        schedule_enabled=safe_get(row_dict, 'schedule_enabled', False),
                        schedule_cron=safe_get(row_dict, 'schedule_cron'),
                        schedule_timezone=safe_get(row_dict, 'schedule_timezone', 'UTC'),
                        bronze_configuration=safe_get(row_dict, 'bronze_configuration'),
                        silver_configuration=safe_get(row_dict, 'silver_configuration')
                    ))
                except Exception as row_e:
                    print(f"Skipping invalid connection record in get_all_connections: {row_e}")
                    continue
            
            # Sort by last updated (descending) in Python
            connections.sort(key=lambda x: x.last_sync_time or "", reverse=True)
            
            return connections
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                if "column" in error_msg.lower():
                    print(f"CRITICAL ERROR: Schema mismatch in configuration_metadata. Missing column? {e}")
                    return []
                return []
            
            print(f"Error loading all connections: {e}")
            raise e

    def update_table_configuration(
        self,
        connection_id: str,
        selected_tables: Dict[str, Any]
    ) -> bool:
        """
        Update the selected_tables configuration for an existing connection.
        Used by the Pipeline Inspector to save per-table load type and watermark config.
        
        Args:
            connection_id: The UUID of the connection to update
            selected_tables: New table config, e.g. {"schema": {"table": {"load_type": "full"}}}
            
        Returns:
            True if update was successful, False otherwise.
        """
        if not connection_id:
            return False
            
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.configuration_metadata"
        
        safe_id = connection_id.strip().replace("'", "''")
        tbl_configuration_json = json.dumps(selected_tables).replace("'", "''")
        
        try:
            update_sql = f"""
                UPDATE {target_table}
                SET tbl_configuration = '{tbl_configuration_json}',
                    updated_at = current_timestamp()
                WHERE connection_id = '{safe_id}'
            """
            self.spark.sql(update_sql).collect()
            
            # Also update local cache if it matches
            cached_data = self._read_from_cache()
            if cached_data and cached_data.get('connection_id') == connection_id:
                cached_data['tbl_configuration'] = tbl_configuration_json
                cached_data['updated_at'] = datetime.datetime.now().isoformat()
                self._save_to_cache(cached_data)
            
            print(f"INFO: Updated table configuration for connection {connection_id}")
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to update table configuration: {e}")
            return False

    @_with_retry
    def fetch_schemas_for_connection(self, connection_id: str, catalog: str = None) -> 'SchemaMetadata':
        """
        Fetch schema/table metadata for an existing connection by its ID.
        
        Resolves secrets automatically so the frontend doesn't need raw config.
        For Databricks, a catalog must be specified.
        
        Args:
            connection_id: UUID of the saved connection
            catalog: (Databricks only) catalog name to browse
            
        Returns:
            SchemaMetadata with schema→tables mapping
        """
        config = self.get_configuration_by_id(connection_id)
        if not config:
            raise ValueError(f"Connection '{connection_id}' not found.")
        
        _log_trace(f"Config loaded for {connection_id}, resolving secrets...")
        resolved = self._resolve_secrets(config.config)
        
        # Inject catalog for Databricks if provided
        if catalog:
            if config.connector_type == "databricks":
                resolved["catalog"] = catalog
            elif config.connector_type == "sqlserver":
                resolved["database"] = catalog
        
        adapter = self.get_adapter(config.connector_type)
        _log_trace(f"Calling adapter.fetch_schemas_and_tables for {config.connector_type}...")
        try:
            return adapter.fetch_schemas_and_tables(self.spark, resolved)
        except Exception as e:
            _log_trace(f"Adapter fetch failed: {e}")
            raise e

    def fetch_all_columns_for_table(
        self,
        connection_id: str,
        schema: str,
        table: str,
        catalog: str = None
    ) -> List[Dict[str, str]]:
        """
        Fetch column definitions for a table from an existing connection by its ID.
        
        Resolves secrets automatically so the frontend doesn't need raw config.
        
        Args:
            connection_id: UUID of the saved connection
            schema: Schema name
            table: Table name
            catalog: (Databricks only) catalog name
            
        Returns:
            List of dicts with 'name' and 'type' keys
        """
        config = self.get_configuration_by_id(connection_id)
        if not config:
            raise ValueError(f"Connection '{connection_id}' not found.")
        
        resolved = self._resolve_secrets(config.config)
        
        if catalog:
            if config.connector_type == "databricks":
                resolved["catalog"] = catalog
            elif config.connector_type == "sqlserver":
                resolved["database"] = catalog
        
        adapter = self.get_adapter(config.connector_type)
        return adapter.fetch_columns(self.spark, resolved, schema, table)

    
    def trigger_ingestion_notebook(self, connection_id: str) -> str:
        """
        Triggers the ingestion notebook for a specific connection.
        
        Args:
            connection_id: The UUID of the connection configuration to process.
            
        Returns:
            Output/Exit value of the notebook execution.
            
        Raises:
            Exception: If notebook execution fails or DBUtils is unavailable.
        """
        print(f"INFO: Triggering ingestion notebook for ID: {connection_id}")
        
        # Determine the correct notebook path
        # Using the standard /Shared location as requested
        notebook_path = "/Shared/ER_aligned/nb_brz_ingestion"
        
        try:
            # Use Databricks SDK to trigger the notebook as a job
            # dbutils.notebook.run is not supported in non-notebook contexts (like Apps/Jobs)
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.jobs import Task, NotebookTask, Source

            w = WorkspaceClient()
            
            print(f"INFO: Submitting one-time run for: {notebook_path}")
            
            # Submit a one-time run
            # This returns a future-like object or waits depending on SDK version usage
            # We use 'submit' which waits for completion by default in some contexts, 
            # or we can use .result() on the returned operation.
            
            # Get configured cluster ID from .streamlit/secrets.toml or env vars
            cluster_id = st.secrets.get("DATABRICKS_CLUSTER_ID")
            if not cluster_id:
                raise ValueError("DATABRICKS_CLUSTER_ID not configured. Set it in .streamlit/secrets.toml or Databricks secret scope.")
            print(f"DEBUG: Using existing_cluster_id={cluster_id} for ingestion task")
            
            run = w.jobs.submit(
                run_name=f"Ingestion_Trigger_{str(connection_id)[:8]}",
                tasks=[
                    Task(
                        task_key="ingestion_task",
                        existing_cluster_id=cluster_id,
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters={"connection_id": connection_id}
                        )
                    )
                ]
            ).result() # Wait for completion
            
            print(f"INFO: Job execution completed. State: {run.state.life_cycle_state}")
            
            if run.state.result_state and run.state.result_state.name == "SUCCESS":
                 # Log the event for success
                self.audit_logger.log_event(
                    user=st.session_state.get("username", "System"),
                    action="Triggered Ingestion",
                    module="Connectors",
                    status="Success",
                    details=f"Triggered {notebook_path} for ID {connection_id}. Run ID: {run.run_id}"
                )
                return f"Success (Run ID: {run.run_id})"
            else:
                 raise Exception(f"Job failed with state: {run.state.result_state}")

        except ImportError:
            # Fallback if SDK is not installed (unlikely in DBX)
            self.audit_logger.log_event(
                user=st.session_state.get("username", "System"),
                action="Triggered Ingestion",
                module="Connectors",
                status="Failed",
                details=f"Failed to trigger {notebook_path}: databricks-sdk not installed."
            )
            raise Exception("databricks-sdk is missing. Cannot trigger ingestion job.")
            
        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: Failed to trigger notebook: {error_msg}")
            
            self.audit_logger.log_event(
                user=st.session_state.get("username", "System"),
                action="Triggered Ingestion",
                module="Connectors",
                status="Failed",
                details=f"Failed to trigger {notebook_path}: {error_msg}"
            )
            raise e



        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: Failed to trigger profiling notebook: {error_msg}")
            
            self.audit_logger.log_event(
                user=st.session_state.get("username", "System"),
                action="Triggered Profiling",
                module="Connectors",
                status="Failed",
                details=f"Failed to trigger {notebook_path}: {error_msg}"
            )
            raise e

    def trigger_profiling_notebook(self, connection_id: str) -> Dict[str, Any]:
        """
        Triggers the profiling notebook for a specific connection and returns the metrics.
        
        Args:
            connection_id: The UUID of the connection configuration to process.
            
        Returns:
            Dictionary containing profiling metrics.
            
        Raises:
            Exception: If notebook execution fails or SDK is unavailable.
        """
        print(f"INFO: Triggering profiling notebook for ID: {connection_id}")
        
        # Determine the correct notebook path
        notebook_path = "/Shared/ER_aligned/nb_mdm_profiling"
        
        try:
            # Import SDK inside method to avoid top-level dependency issues
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.jobs import Task, NotebookTask, Source

            w = WorkspaceClient()
            
            print(f"INFO: Submitting one-time run for: {notebook_path}")
            
            # Submit a one-time run and wait for the result
            # Get configured cluster ID from .streamlit/secrets.toml or env vars
            cluster_id = st.secrets.get("DATABRICKS_CLUSTER_ID")
            if not cluster_id:
                raise ValueError("DATABRICKS_CLUSTER_ID not configured. Set it in .streamlit/secrets.toml or Databricks secret scope.")
            
            run_output = w.jobs.submit(
                run_name=f"Profiling_Trigger_{str(connection_id)[:8]}",
                tasks=[
                    Task(
                        task_key="profiling_task",
                        existing_cluster_id=cluster_id,
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters={"connection_id": connection_id}
                        )
                    )
                ]
            ).result() 
            
            print(f"INFO: Job execution completed. State: {run_output.state.life_cycle_state}")
            
            if run_output.state.result_state and run_output.state.result_state.name == "SUCCESS":
                # Retrieve the notebook output
                job_run = w.jobs.get_run(run_output.run_id)
                task_run_id = job_run.tasks[0].run_id
                
                output = w.jobs.get_run_output(task_run_id)
                
                # Check for notebook output
                if output.notebook_output and output.notebook_output.result:
                    result_json = output.notebook_output.result
                    print(f"INFO: Notebook returned: {result_json[:500]}...") # Truncate for logging
                    
                     # Log success
                    self.audit_logger.log_event(
                        user=st.session_state.get("username", "System"),
                        action="Triggered Profiling",
                        module="Connectors",
                        status="Success",
                        details=f"Triggered {notebook_path} for ID {connection_id}. Run ID: {run_output.run_id}"
                    )
                    
                    try:
                        return json.loads(result_json)
                    except json.JSONDecodeError as je:
                        print(f"ERROR: Failed to decode notebook output: {je}")
                        return {"status": "error", "message": "Invalid JSON from notebook", "raw": result_json}
                        
                else:
                    print("WARN: Notebook completed but returned no output.")
                    return {"status": "success", "message": "No output returned"}
                
            else:
                 raise Exception(f"Job failed with state: {run_output.state.result_state}")

        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: Failed to trigger profiling notebook: {error_msg}")
            
            self.audit_logger.log_event(
                user=st.session_state.get("username", "System"),
                action="Triggered Profiling",
                module="Connectors",
                status="Failed",
                details=f"Failed to trigger {notebook_path}: {error_msg}"
            )
            raise e


def get_connector_service(spark=None) -> ConnectorService:
    """
    Get the connector service instance (singleton pattern).
    
    Args:
        spark: Optional SparkSession to use
        
    Returns:
        ConnectorService instance
    """
    global _service_instance
    if _service_instance is None:
        _service_instance = ConnectorService(spark)
    return _service_instance


def reset_connector_service():
    """
    Reset the connector service singleton.
    
    Call this after code changes to force re-registration of adapters.
    Useful during development when adding new connectors.
    """
    global _service_instance
    _service_instance = None
    # Clear any cached adapters at the class level
    ConnectorService._adapters = {}
