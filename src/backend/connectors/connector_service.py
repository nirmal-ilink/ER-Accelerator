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
import streamlit as st
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

# Import adapters
from .adapters import SQLServerAdapter
from .adapters.base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


@dataclass
class ConnectorConfig:
    """Stored connector configuration."""
    connector_type: str
    connector_name: str
    config: Dict[str, Any]
    selected_tables: Dict[str, List[str]]  # schema -> [tables]
    status: str  # 'active', 'inactive', 'error'
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
        self._register_adapters()
    
    def _register_adapters(self):
        """Register all available connector adapters."""
        # SQL Server
        sql_adapter = SQLServerAdapter()
        self._adapters[sql_adapter.connector_type] = sql_adapter
        
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
    
    def _refresh_spark_session(self):
        """Force refresh of the underlying Spark session."""
        try:
            from src.backend.bootstrapper import get_bootstrapper
            print("INFO: Refreshing ConnectorService Spark session...")
            self._spark = get_bootstrapper().reset_spark()
        except Exception as e:
            print(f"ERROR: Failed to refresh Spark session: {e}")

    def test_connection(
        self, 
        connector_type: str, 
        config: Dict[str, Any]
    ) -> ConnectionTestResult:
        """
        Test connection to a data source.
        
        Args:
            connector_type: Type of connector (e.g., 'sqlserver')
            config: Connection configuration dictionary
            
        Returns:
            ConnectionTestResult with success status and details
        """
        adapter = self.get_adapter(connector_type)
        
        # First attempt
        result = adapter.test_connection(self.spark, config)
        
        # Check for invalid session error (Databricks Connect specific)
        # Check both message and raw_error details since adapters might sanitize the message
        error_indicators = [str(result.message)]
        if result.details and "raw_error" in result.details:
            error_indicators.append(str(result.details["raw_error"]))
            
        is_session_error = any("[NO_ACTIVE_SESSION]" in err for err in error_indicators)
        
        if not result.success and is_session_error:
            print("WARNING: Caught [NO_ACTIVE_SESSION]. Attempting to refresh Spark session...")
            self._refresh_spark_session()
            
            # Retry with new session
            result = adapter.test_connection(self.spark, config)
            
        return result
    
    def fetch_metadata(
        self, 
        connector_type: str, 
        config: Dict[str, Any]
    ) -> SchemaMetadata:
        """
        Fetch schemas and tables from a data source.
        
        Args:
            connector_type: Type of connector (e.g., 'sqlserver')
            config: Connection configuration dictionary
            
        Returns:
            SchemaMetadata with schema->tables mapping
        """
        adapter = self.get_adapter(connector_type)
        return adapter.fetch_schemas_and_tables(self.spark, config)

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
        adapter = self.get_adapter(connector_type)
        return adapter.fetch_columns(self.spark, config, schema, table)
    
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
        
        Args:
            connector_type: Type of connector
            config: Original configuration with raw secrets
            
        Returns:
            Configuration with secrets replaced by pointers
        """
        secret_fields = ['password', 'token', 'key', 'secret']
        processed_config = {}
        
        for key, value in config.items():
            is_secret = any(sf in key.lower() for sf in secret_fields)
            
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
        selected_tables: Dict[str, List[str]],
        status: str = "active",
        load_type: str = "full",
        watermark_column: Optional[str] = None,
        schedule_enabled: bool = False,
        schedule_cron: Optional[str] = None,
        schedule_timezone: str = "UTC"
    ) -> bool:
        """
        Save connector configuration to ingestion_metadata table.
        """
        # Store secrets and get config with pointers
        safe_config = self._store_secrets(connector_type, config)
        
        # Prepare data for new schema
        # connection_id mapped to connector_type for now to maintain singleton pattern per type
        connection_id = connector_type 
        source_type = connector_type
        
        # Filter out meta fields from config json
        meta_fields = ['selected_tables', 'load_type', 'watermark_column', 'schedule_enabled', 'schedule_cron', 'schedule_timezone']
        configuration = {k: v for k, v in safe_config.items() if k not in meta_fields and k != 'selected_tables'}
        
        # JSON serialization
        configuration_json = json.dumps(configuration).replace("'", "''")
        selected_tables_json = json.dumps(selected_tables).replace("'", "''")
        
        # Get target table location
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        def escape_sql(v):
            if v is None:
                return "NULL"
            val = str(v).replace("'", "''")
            return f"'{val}'"
        
        # Use MERGE to upsert
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT 
                '{connection_id}' as connection_id,
                '{source_type}' as source_type,
                {escape_sql(connector_name)} as source_name,
                '{configuration_json}' as configuration,
                '{selected_tables_json}' as selected_tables,
                {escape_sql(load_type)} as load_type,
                {escape_sql(watermark_column)} as watermark_column,
                {str(schedule_enabled).lower()} as schedule_enabled,
                {escape_sql(schedule_cron)} as schedule_cron,
                {escape_sql(schedule_timezone)} as schedule_timezone,
                '{status}' as status,
                current_timestamp() as updated_at,
                current_timestamp() as created_at
            ) AS source
            ON target.connection_id = source.connection_id
            WHEN MATCHED THEN
                UPDATE SET
                    source_name = source.source_name,
                    configuration = source.configuration,
                    selected_tables = source.selected_tables,
                    load_type = source.load_type,
                    watermark_column = source.watermark_column,
                    schedule_enabled = source.schedule_enabled,
                    schedule_cron = source.schedule_cron,
                    schedule_timezone = source.schedule_timezone,
                    status = source.status,
                    updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    connection_id, source_type, source_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    status, created_at, updated_at
                )
                VALUES (
                    source.connection_id, source.source_type, source.source_name, source.configuration, source.selected_tables,
                    source.load_type, source.watermark_column, source.schedule_enabled, source.schedule_cron, source.schedule_timezone,
                    source.status, source.created_at, source.updated_at
                )
        """
        
        try:
            self.spark.sql(merge_sql).collect()
            return True
        except Exception as e:
            error_msg = str(e)
            
            # If table doesn't exist, try to create it
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                self._create_metadata_table(target_catalog, target_schema, target_table)
                # Retry the merge
                self.spark.sql(merge_sql).collect()
                return True
            else:
                raise
    
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
                configuration STRING COMMENT 'Connection parameters in JSON format',
                selected_tables STRING COMMENT 'Selected schemas and tables in JSON format',
                load_type STRING COMMENT 'full or incremental',
                watermark_column STRING,
                schedule_enabled BOOLEAN,
                schedule_cron STRING,
                schedule_timezone STRING,
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
    
    def load_configuration(
        self, 
        connector_type: str
    ) -> Optional[ConnectorConfig]:
        """
        Load saved configuration for a connector type.
        
        Args:
            connector_type: Type of connector to load
            
        Returns:
            ConnectorConfig if found, None otherwise
        """
        target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
        target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
        target_table = f"{target_catalog}.{target_schema}.ingestion_metadata"
        
        try:
            # Query the new schema columns
            df = self.spark.sql(f"""
                SELECT 
                    connection_id, source_type, source_name, configuration, selected_tables,
                    load_type, watermark_column, schedule_enabled, schedule_cron, schedule_timezone,
                    status, updated_at
                FROM {target_table}
                WHERE connection_id = '{connector_type}'
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
                connector_type=row['source_type'],
                connector_name=row['source_name'],
                config=config_dict,
                selected_tables=selected_tables,
                status=row['status'] or 'inactive',
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


# Factory function for easy access
_service_instance: Optional[ConnectorService] = None

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
