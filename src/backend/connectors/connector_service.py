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
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

# Constants
CACHE_FILE_NAME = "connector_cache.json"

# Module-level singleton instance
_service_instance = None

# Import Audit Logger
from src.backend.audit.logger import AuditLogger

# Import adapters
from .adapters import SQLServerAdapter, DatabricksAdapter
from .adapters.base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


@dataclass
class ConnectorConfig:
    """Stored connector configuration."""
    connection_id: str  # Unique ID
    connector_type: str
    connector_name: str
    connection_name: str  # User-given name (unique per user)
    config: Dict[str, Any]
    selected_tables: Dict[str, Any]  # schema -> {table: {load_type, watermark_column}} or legacy list
    status: str  # 'active', 'inactive', 'error'
    created_by: str = "System"  # Username who created this connection
    # Load configuration
    load_type: str = "full"  # 'full' or 'incremental'
    watermark_column: Optional[str] = None  # e.g., 'updated_at'
    last_sync_time: Optional[str] = None  # ISO timestamp
    # Scheduling
    schedule_enabled: bool = False
    schedule_cron: Optional[str] = None  # e.g., '0 2 * * *'
    schedule_timezone: str = "UTC"
    # Sync tracking
    sync_status: str = "pending"  # 'pending', 'running', 'success', 'failed'


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
        # SQL Server
        sql_adapter = SQLServerAdapter()
        self._adapters[sql_adapter.connector_type] = sql_adapter
        
        # Databricks / Unity Catalog
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
        if hasattr(adapter, 'fetch_catalogs'):
            return adapter.fetch_catalogs(self.spark, config)
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
        for key, value in config.items():
            if isinstance(value, str) and value.startswith("databricks://secrets/"):
                # This is a pointer - secrets are read at runtime by Spark
                # For now, keep the pointer as-is (Spark reads from secrets)
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
    
    def save_configuration(
        self,
        connector_type: str,
        connector_name: str,
        config: Dict[str, Any],
        connection_name: str = "",
        created_by: str = "System",
        selected_tables: Optional[Dict[str, Any]] = None,
        status: str = "active",
        load_type: str = "full",
        watermark_column: Optional[str] = None,
        schedule_enabled: bool = False,
        schedule_cron: Optional[str] = None,
        schedule_timezone: str = "UTC"
    ) -> str:
        """
        Save connector configuration to ingestion_metadata table.
        Optimized for UI responsiveness:
        1. Stores secrets (Synchronous - fast)
        2. Saves to local cache (Synchronous - fast)
        3. Writes to Delta Table (Asynchronous - background thread)
        Returns connection_id immediately.
        """
        if selected_tables is None:
            selected_tables = {}
        
        # Store secrets and get config with pointers
        safe_config = self._store_secrets(connector_type, config)
        
        # Generate unique connection ID using UUID
        connection_id = str(uuid.uuid4())
        source_type = connector_type
        
        # Filter out meta fields from config json
        meta_fields = ['selected_tables', 'load_type', 'watermark_column', 'schedule_enabled', 'schedule_cron', 'schedule_timezone']
        configuration = {k: v for k, v in safe_config.items() if k not in meta_fields and k != 'selected_tables'}
        
        # JSON serialization
        configuration_json = json.dumps(configuration).replace("'", "''")
        selected_tables_json = json.dumps(selected_tables).replace("'", "''")
        
        # Update Local Cache - IMMEDIATE UI FEEDBACK
        cache_data = {
            "connection_id": connection_id,
            "source_type": source_type,
            "source_name": connector_name,
            "connection_name": connection_name,
            "created_by": created_by,
            "configuration": configuration_json,
            "selected_tables": selected_tables_json,
            "load_type": load_type,
            "watermark_column": watermark_column,
            "schedule_enabled": schedule_enabled,
            "schedule_cron": schedule_cron,
            "schedule_timezone": schedule_timezone,
            "status": status,
            "updated_at": datetime.datetime.now().isoformat()
        }
        self._save_to_cache(cache_data)
        
        # Launch background thread for heavy Spark write
        # This prevents the UI from freezing
        thread = threading.Thread(
            target=self._persist_to_delta_async,
            kwargs={
                "connection_id": connection_id,
                "connector_type": connector_type,
                "connector_name": connector_name,
                "connection_name": connection_name,
                "created_by": created_by,
                "configuration_json": configuration_json,
                "selected_tables_json": selected_tables_json,
                "load_type": load_type,
                "watermark_column": watermark_column,
                "schedule_enabled": schedule_enabled,
                "schedule_cron": schedule_cron,
                "schedule_timezone": schedule_timezone,
                "status": status,
                "user": created_by
            }
        )
        thread.daemon = True # Ensure thread doesn't block app exit
        thread.start()
        
        return connection_id

    def _persist_to_delta_async(
        self,
        connection_id: str,
        connector_type: str,
        connector_name: str,
        connection_name: str,
        created_by: str,
        configuration_json: str,
        selected_tables_json: str,
        load_type: str,
        watermark_column: Optional[str],
        schedule_enabled: bool,
        schedule_cron: Optional[str],
        schedule_timezone: str,
        status: str,
        user: str
    ):
        """
        Background worker to write configuration to Delta Checkpoint.
        """
        print(f"INFO: [Async Save] Starting background persistence for {connection_id}...")
        
        # Get target table location
        # NOTE: st.secrets might handle threading differently, but usually works if read-only
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        def escape_sql(v):
            if v is None:
                return "NULL"
            val = str(v).replace("'", "''")
            return f"'{val}'"
        
        # Insert new record (Immutable Ledger style)
        insert_sql = f"""
            INSERT INTO {target_table} (
                connection_id, source_type, source_name, connection_name, configuration, selected_tables,
                load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                created_by, status, created_at, updated_at
            )
            VALUES (
                '{connection_id}', '{connector_type}', {escape_sql(connector_name)}, {escape_sql(connection_name)}, '{configuration_json}', '{selected_tables_json}',
                {escape_sql(load_type)}, {escape_sql(watermark_column)}, {str(schedule_enabled).lower()}, {escape_sql(schedule_cron)}, {escape_sql(schedule_timezone)},
                {escape_sql(created_by)}, '{status}', current_timestamp(), current_timestamp()
            )
        """
        
        try:
            self.spark.sql(insert_sql).collect()
            
            self.audit_logger.log_event(
                user=user,
                action=f"Saved Configuration (Async)",
                module="Connectors",
                status="Success",
                details=f"Saved {connector_type} configuration '{connector_name}' (ID: {connection_id})"
            )
            print(f"INFO: [Async Save] Successfully wrote {connection_id} to Delta table.")
            
        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: [Async Save] Failed to write {connection_id}: {error_msg}")
            
            # CASE 1: Session Death (Databricks Connect timeout)
            if "[NO_ACTIVE_SESSION]" in error_msg:
                print("WARNING: Spark Session is not active. Attempting to re-initialize...")
                
                # Re-initialize Spark Session
                try:
                    from src.backend.bootstrapper import get_bootstrapper
                    self._spark = get_bootstrapper().reset_spark()
                    
                    self.spark.sql(insert_sql).collect()
                    
                    # Log recovery success
                    self.audit_logger.log_event(
                        user=user,
                        action=f"Saved Configuration (Async)",
                        module="Connectors",
                        status="Success",
                        details=f"Saved {connector_type} configuration '{connector_name}' (ID: {connection_id}) after session recovery"
                    )
                    print(f"INFO: [Async Save] Recovered and wrote {connection_id}.")
                except Exception as retry_e:
                    # Check if retry failed due to missing table (corner case)
                    retry_msg = str(retry_e)
                    if "TABLE_OR_VIEW_NOT_FOUND" in retry_msg or "does not exist" in retry_msg.lower():
                        self._create_metadata_table(target_catalog, target_schema, target_table)
                        self.spark.sql(insert_sql).collect()
                        print(f"INFO: [Async Save] Created table and wrote {connection_id}.")
                    else:
                        print(f"ERROR: [Async Save] Recovery failed for {connection_id}: {retry_e}")

            # CASE 2: Missing Table (First run)
            elif "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                self._create_metadata_table(target_catalog, target_schema, target_table)
                # Retry the insert
                self.spark.sql(insert_sql).collect()
                
                # Log success after retry
                self.audit_logger.log_event(
                    user=user,
                    action=f"Saved Configuration (Async)",
                    module="Connectors",
                    status="Success",
                    details=f"Saved {connector_type} configuration '{connector_name}' (ID: {connection_id})"
                )
                print(f"INFO: [Async Save] Created table and wrote {connection_id}.")
            
            else:
                # Log failure
                self.audit_logger.log_event(
                    user=user,
                    action=f"Saved Configuration (Async)",
                    module="Connectors",
                    status="Failed",
                    details=f"Failed to save {connector_type} configuration: {error_msg}"
                )
    
    def _create_metadata_table(
        self, 
        catalog: str, 
        schema: str, 
        table: str
    ):
        """
        Create the ingestion_metadata table if it doesn't exist.
        """
        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        
        # Create the table with enhanced schema
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                connection_id STRING COMMENT 'Unique identifier for the connection',
                source_type STRING COMMENT 'Type of data source (e.g. sqlserver)',
                source_name STRING COMMENT 'Display name of the source',
                connection_name STRING COMMENT 'User-given connection name, unique per user',
                configuration STRING COMMENT 'Connection parameters in JSON format',
                selected_tables STRING COMMENT 'Selected schemas and tables in JSON format',
                load_type STRING COMMENT 'full or incremental',
                watermark_column STRING,
                schedule_enabled BOOLEAN,
                schedule_cron STRING,
                schedule_timezone STRING,
                created_by STRING COMMENT 'Username who created this connection',
                status STRING COMMENT 'active, inactive',
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) 
            USING DELTA
            COMMENT 'Stores metadata and configuration for data ingestion pipelines'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """
        self.spark.sql(create_sql)
        
        # Add new columns to existing tables (ALTER TABLE is idempotent for IF NOT EXISTS-like behavior)
        try:
            self.spark.sql(f"ALTER TABLE {table} ADD COLUMNS (connection_name STRING COMMENT 'User-given connection name')")
        except Exception:
            pass  # Column already exists
        try:
            self.spark.sql(f"ALTER TABLE {table} ADD COLUMNS (created_by STRING COMMENT 'Username who created this connection')")
        except Exception:
            pass  # Column already exists
    
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
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        try:
            # Query the latest configuration for this source_type
            df = self.spark.sql(f"""
                SELECT 
                    connection_id, source_type, source_name, connection_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    created_by, status, updated_at
                FROM {target_table}
                WHERE source_type = '{connector_type}'
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            
            rows = df.collect()
            if not rows:
                return None
            
            row = rows[0]
            
            # Parse JSON fields
            config_dict = json.loads(row['configuration'])
            try:
                selected_tables = json.loads(row['selected_tables'])
            except:
                selected_tables = {}
            
            return ConnectorConfig(
                connection_id=row['connection_id'],
                connector_type=row['source_type'],
                connector_name=row['source_name'],
                connection_name=row.get('connection_name') or '',
                config=config_dict,
                selected_tables=selected_tables,
                status=row['status'] or 'inactive',
                created_by=row.get('created_by') or 'System',
                load_type=row['load_type'],
                watermark_column=row['watermark_column'],
                last_sync_time=row['updated_at'].isoformat() if row['updated_at'] else None,
                schedule_enabled=row['schedule_enabled'],
                schedule_cron=row['schedule_cron'],
                schedule_timezone=row['schedule_timezone']
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
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        # 1. Try Local Cache First (Fast Path)
        cached_data = self._read_from_cache()
        if cached_data:
            try:
                config_dict = json.loads(cached_data['configuration']) if isinstance(cached_data['configuration'], str) else cached_data['configuration']
                
                selected_tables = {}
                if 'selected_tables' in cached_data:
                     st_val = cached_data['selected_tables']
                     selected_tables = json.loads(st_val) if isinstance(st_val, str) else st_val

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
                    load_type=cached_data.get('load_type', 'full'),
                    watermark_column=cached_data.get('watermark_column'),
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
                    connection_id, source_type, source_name, connection_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    created_by, status, updated_at
                FROM {target_table}
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            
            rows = df.collect()
            if not rows:
                return None
            
            row = rows[0]
            
            # Parse JSON fields
            config_dict = json.loads(row['configuration'])
            try:
                selected_tables = json.loads(row['selected_tables'])
            except:
                selected_tables = {}
            
            return ConnectorConfig(
                connection_id=row['connection_id'],
                connector_type=row['source_type'],
                connector_name=row['source_name'],
                connection_name=row.get('connection_name') or '',
                config=config_dict,
                selected_tables=selected_tables,
                status=row['status'] or 'inactive',
                created_by=row.get('created_by') or 'System',
                load_type=row['load_type'],
                watermark_column=row['watermark_column'],
                last_sync_time=row['updated_at'].isoformat() if row['updated_at'] else None,
                schedule_enabled=row['schedule_enabled'],
                schedule_cron=row['schedule_cron'],
                schedule_timezone=row['schedule_timezone']
            )
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return None
            print(f"Error loading latest configuration: {e}")
            return None

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
            
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        # Sanitize the connection_id to prevent SQL injection
        safe_id = connection_id.strip().replace("'", "''")
        
        print(f"DEBUG: Looking for connection_id='{safe_id}' in {target_table}")
        
        try:
            # First, let's see what connection IDs exist in the table
            all_ids_df = self.spark.sql(f"""
                SELECT connection_id, source_name, updated_at
                FROM {target_table}
                ORDER BY updated_at DESC
                LIMIT 10
            """)
            all_ids = all_ids_df.collect()
            print(f"DEBUG: Available connection IDs in database:")
            for row in all_ids:
                print(f"  - {row['connection_id']} ({row['source_name']})")
            
            df = self.spark.sql(f"""
                SELECT 
                    connection_id, source_type, source_name, connection_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    created_by, status, updated_at
                FROM {target_table}
                WHERE connection_id = '{safe_id}'
                LIMIT 1
            """)
            
            rows = df.collect()
            if not rows:
                print(f"DEBUG: No rows found for connection_id='{safe_id}'")
                return None
            
            row = rows[0]
            
            # Parse JSON fields
            config_dict = json.loads(row['configuration'])
            try:
                selected_tables = json.loads(row['selected_tables'])
            except:
                selected_tables = {}
            
            return ConnectorConfig(
                connection_id=row['connection_id'],
                connector_type=row['source_type'],
                connector_name=row['source_name'],
                connection_name=row.get('connection_name') or '',
                config=config_dict,
                selected_tables=selected_tables,
                status=row['status'] or 'inactive',
                created_by=row.get('created_by') or 'System',
                load_type=row['load_type'],
                watermark_column=row['watermark_column'],
                last_sync_time=row['updated_at'].isoformat() if row['updated_at'] else None,
                schedule_enabled=row['schedule_enabled'],
                schedule_cron=row['schedule_cron'],
                schedule_timezone=row['schedule_timezone']
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
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        safe_name = connection_name.strip().replace("'", "''")
        safe_user = created_by.strip().replace("'", "''")
        
        try:
            df = self.spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM {target_table}
                WHERE LOWER(connection_name) = LOWER('{safe_name}')
                  AND LOWER(created_by) = LOWER('{safe_user}')
            """)
            rows = df.collect()
            return rows[0]['cnt'] > 0 if rows else False
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return False
            print(f"WARNING: check_connection_name_exists failed: {e}")
            return False

    def get_user_connections(self, username: str) -> List[ConnectorConfig]:
        """
        Fetch all connections created by a specific user.
        
        Args:
            username: The username to filter by
            
        Returns:
            List of ConnectorConfig objects for the user's connections.
        """
        if not username:
            return []
            
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        safe_user = username.strip().replace("'", "''")
        
        try:
            df = self.spark.sql(f"""
                SELECT 
                    connection_id, source_type, source_name, connection_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    created_by, status, updated_at
                FROM {target_table}
                WHERE LOWER(created_by) = LOWER('{safe_user}')
                ORDER BY updated_at DESC
            """)
            
            rows = df.collect()
            connections = []
            for row in rows:
                try:
                    config_dict = json.loads(row['configuration'])
                except:
                    config_dict = {}
                try:
                    selected_tables = json.loads(row['selected_tables'])
                except:
                    selected_tables = {}
                
                connections.append(ConnectorConfig(
                    connection_id=row['connection_id'],
                    connector_type=row['source_type'],
                    connector_name=row['source_name'],
                    connection_name=row.get('connection_name') or '',
                    config=config_dict,
                    selected_tables=selected_tables,
                    status=row['status'] or 'inactive',
                    created_by=row.get('created_by') or 'System',
                    load_type=row['load_type'],
                    watermark_column=row['watermark_column'],
                    last_sync_time=row['updated_at'].isoformat() if row['updated_at'] else None,
                    schedule_enabled=row['schedule_enabled'],
                    schedule_cron=row['schedule_cron'],
                    schedule_timezone=row['schedule_timezone']
                ))
            
            return connections
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                return []
            print(f"Error loading user connections: {e}")
            return []

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
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        safe_id = connection_id.strip().replace("'", "''")
        selected_tables_json = json.dumps(selected_tables).replace("'", "''")
        
        try:
            update_sql = f"""
                UPDATE {target_table}
                SET selected_tables = '{selected_tables_json}',
                    updated_at = current_timestamp()
                WHERE connection_id = '{safe_id}'
            """
            self.spark.sql(update_sql).collect()
            
            # Also update local cache if it matches
            cached_data = self._read_from_cache()
            if cached_data and cached_data.get('connection_id') == connection_id:
                cached_data['selected_tables'] = selected_tables_json
                cached_data['updated_at'] = datetime.datetime.now().isoformat()
                self._save_to_cache(cached_data)
            
            print(f"INFO: Updated table configuration for connection {connection_id}")
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to update table configuration: {e}")
            return False

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
        
        resolved = self._resolve_secrets(config.config)
        
        # Inject catalog for Databricks if provided
        if catalog and config.connector_type == "databricks":
            resolved["catalog"] = catalog
        
        adapter = self.get_adapter(config.connector_type)
        return adapter.fetch_schemas_and_tables(self.spark, resolved)

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
        
        if catalog and config.connector_type == "databricks":
            resolved["catalog"] = catalog
        
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
                run_name=f"Ingestion_Trigger_{connection_id[:8]}",
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
                run_name=f"Profiling_Trigger_{connection_id[:8]}",
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
                # The run_output object from .result() contains the run_id, but to get the output we need to call get_run_output
                # However, for single-task runs, the run_id of the task run is what we need for get_run_output?
                # Actually w.jobs.submit returns a Run object representing the *job run*.
                # We need the task run ID to get the output.
                
                # Fetch the detailed run info to get task run IDs
                job_run = w.jobs.get_run(run_output.run_id)
                task_run_id = job_run.tasks[0].run_id
                
                output = w.jobs.get_run_output(task_run_id)
                
                # Check for notebook output
                if output.notebook_output and output.notebook_output.result:
                    result_json = output.notebook_output.result
                    print(f"INFO: Notebook returned: {result_json}")
                    
                     # Log success
                    self.audit_logger.log_event(
                        user=st.session_state.get("username", "System"),
                        action="Triggered Profiling",
                        module="Connectors",
                        status="Success",
                        details=f"Triggered {notebook_path} for ID {connection_id}. Run ID: {run_output.run_id}"
                    )
                    
                    return json.loads(result_json)
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
