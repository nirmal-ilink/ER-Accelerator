"""
Databricks Connector Adapter - Implementation for Databricks/Unity Catalog.

Uses the databricks-sql-connector Python package for metadata discovery,
avoiding the need for JDBC driver installation on the cluster.
"""

import time
from typing import Dict, List, Any
from .base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


class DatabricksAdapter(BaseConnectorAdapter):
    """
    Databricks adapter for Unity Catalog metadata discovery.
    
    Uses the databricks-sql-connector Python package which is simpler
    than JDBC and works directly from the Databricks runtime.
    
    Supports:
    - Azure Databricks
    - AWS Databricks  
    - GCP Databricks
    
    Requires: pip install databricks-sql-connector
    """
    
    # Connection timeout in seconds
    DEFAULT_TIMEOUT = 30
    
    @property
    def connector_type(self) -> str:
        return "databricks"
    
    @property
    def display_name(self) -> str:
        return "Databricks"
    
    @property
    def required_fields(self) -> List[str]:
        # Catalog is not required for initial connection - it's selected from dropdown
        return ["Host", "Token", "HTTP Path"]
    
    @property
    def requires_spark_for_test(self) -> bool:
        return False
    
    def _get_connection(self, config: Dict[str, Any], timeout: int = 30):
        """
        Create a connection to Databricks SQL Warehouse.
        
        Args:
            config: Connection configuration
            timeout: Socket timeout in seconds
            
        Returns:
            databricks.sql.Connection object
        """
        from databricks import sql
        
        host = config.get('host', '').strip()
        token = config.get('token', '').strip()
        http_path = config.get('http_path', '').strip()
        
        # Handle common malformed host values
        # Remove $ prefix (sometimes added from secret references)
        if host.startswith('$'):
            host = host[1:]
        
        # Remove https:// if user included it
        if host.startswith('https://'):
            host = host[8:]
        if host.startswith('http://'):
            host = host[7:]
            
        # Robust cleanup for HTTP Path (handle full URLs, quotes, query params)
        if http_path:
            # Strip quotes
            http_path = http_path.strip('"\'')
            
            try:
                from urllib.parse import urlparse
                # Handle full URLs or paths with query parameters
                if '://' in http_path or '?' in http_path:
                    parsed = urlparse(http_path)
                    if parsed.path:
                        http_path = parsed.path
            except Exception:
                pass
            
            # Ensure it starts with /
            if not http_path.startswith('/'):
                http_path = '/' + http_path
        
        return sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            _socket_timeout=timeout
        )

    def build_jdbc_url(self, config: Dict[str, Any]) -> str:
        """Not used - kept for interface compatibility."""
        return ""
    
    def get_jdbc_properties(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Not used - kept for interface compatibility."""
        return {}
    
    def get_jdbc_driver_class(self) -> str:
        """Not used - kept for interface compatibility."""
        return ""
    
    def fetch_catalogs(self, spark, config: Dict[str, Any]) -> List[str]:
        """
        Fetches all available catalogs from the Databricks workspace.
        
        Args:
            spark: SparkSession (not used, kept for interface compatibility)
            config: Connection configuration (host, token, http_path)
            
        Returns:
            List of catalog names
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection = self._get_connection(config)
            cursor = connection.cursor()
            
            cursor.execute("SHOW CATALOGS")
            rows = cursor.fetchall()
            
            catalogs = []
            for row in rows:
                # First column contains catalog name
                if row and len(row) > 0:
                    catalogs.append(row[0])
            
            cursor.close()
            connection.close()
            
            return sorted(catalogs)
            
        except Exception as e:
            error_msg = str(e)
            
            if "Invalid access token" in error_msg or "401" in error_msg or "Unauthorized" in error_msg:
                raise ConnectionError(
                    "Authentication failed. Please check your Personal Access Token."
                ) from e
            elif "Could not find" in error_msg or "not found" in error_msg.lower():
                raise ConnectionError(
                    "Invalid HTTP Path. Please check your SQL Warehouse or cluster HTTP path."
                ) from e
            elif "Network" in error_msg or "timeout" in error_msg.lower() or "getaddrinfo" in error_msg:
                raise ConnectionError(
                    f"Could not connect to '{config.get('host')}'. "
                    "Please check the workspace URL."
                ) from e
            elif "ModuleNotFoundError" in error_msg or "No module named" in error_msg:
                raise ConnectionError(
                    "databricks-sql-connector package not installed. "
                    "Please add 'databricks-sql-connector' to requirements.txt"
                ) from e
            else:
                raise ConnectionError(f"Failed to fetch catalogs: {error_msg}") from e

    def fetch_schemas_and_tables(self, spark, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Fetches all schemas and their tables from the specified Databricks catalog.
        
        Args:
            spark: SparkSession (not used, kept for interface compatibility)
            config: Connection configuration (must include 'catalog')
            
        Returns:
            SchemaMetadata with schema->tables mapping
            
        Raises:
            ConnectionError: If connection fails
        """
        start_time = time.time()
        
        catalog = config.get('catalog', '').strip()
        if not catalog:
            raise ValueError("Catalog is required. Please select a catalog first.")
        
        try:
            connection = self._get_connection(config)
            cursor = connection.cursor()
            
            # Step 1: Fetch all schemas in the catalog
            cursor.execute(f"SHOW SCHEMAS IN {catalog}")
            schema_rows = cursor.fetchall()
            
            schema_names = []
            for row in schema_rows:
                if row and len(row) > 0:
                    schema_name = row[0]
                    if schema_name and schema_name.lower() not in ['information_schema']:
                        schema_names.append(schema_name)
            
            # Step 2: For each schema, fetch tables
            schemas: Dict[str, List[str]] = {}
            
            for schema_name in schema_names:
                try:
                    cursor.execute(f"SHOW TABLES IN {catalog}.{schema_name}")
                    table_rows = cursor.fetchall()
                    
                    tables = []
                    for row in table_rows:
                        # Table name is typically in second column (first is database)
                        if row and len(row) > 1:
                            table_name = row[1]
                        elif row and len(row) > 0:
                            table_name = row[0]
                        else:
                            continue
                            
                        if table_name:
                            tables.append(table_name)
                    
                    if tables:
                        schemas[schema_name] = sorted(tables)
                        
                except Exception as e:
                    # Log but continue with other schemas
                    print(f"Warning: Could not fetch tables for schema {schema_name}: {e}")
                    continue
            
            cursor.close()
            connection.close()
            
            fetch_time = time.time() - start_time
            
            return SchemaMetadata(
                schemas=schemas,
                total_schemas=len(schemas),
                total_tables=sum(len(tables) for tables in schemas.values()),
                fetch_time_seconds=round(float(fetch_time), 2)
            )
            
        except Exception as e:
            error_msg = str(e)
            
            if "Invalid access token" in error_msg or "401" in error_msg:
                raise ConnectionError(
                    "Authentication failed. Please check your Personal Access Token."
                ) from e
            elif "CATALOG_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                raise ConnectionError(
                    f"Catalog '{catalog}' not found. Please check the catalog name."
                ) from e
            elif "Could not find" in error_msg or "not found" in error_msg.lower():
                raise ConnectionError(
                    "Invalid HTTP Path. Please check your SQL Warehouse or cluster HTTP path."
                ) from e
            else:
                raise ConnectionError(f"Connection failed: {error_msg}") from e

    def fetch_columns(self, spark, config: Dict[str, Any], schema: str, table: str) -> List[Dict[str, str]]:
        """
        Fetches column definitions for a specific table from Databricks.
        
        Args:
            spark: SparkSession (not used)
            config: Connection configuration
            schema: Schema name
            table: Table name
            
        Returns:
            List of dictionaries containing 'name' and 'type' keys
        """
        catalog = config.get('catalog', '').strip()
        
        try:
            connection = self._get_connection(config)
            cursor = connection.cursor()
            
            cursor.execute(f"DESCRIBE TABLE {catalog}.{schema}.{table}")
            rows = cursor.fetchall()
            
            columns = []
            for row in rows:
                if row and len(row) >= 2:
                    col_name = row[0]
                    data_type = row[1]
                    
                    # Skip partition info and empty rows
                    if col_name and not col_name.startswith('#') and col_name.strip():
                        columns.append({"name": col_name.strip(), "type": data_type})
            
            cursor.close()
            connection.close()
            
            return columns
            
        except Exception as e:
            print(f"Error fetching columns for {catalog}.{schema}.{table}: {e}")
            raise ConnectionError(f"Failed to fetch columns: {str(e)}")
    
    def test_connection(self, spark, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Tests connection to Databricks SQL Warehouse with a simple query.
        
        Args:
            spark: SparkSession (not used)
            config: Connection configuration (host, token, http_path)
            
        Returns:
            ConnectionTestResult with success/failure details
        """
        start_time = time.time()
        
        # Validate basic connection fields
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            return ConnectionTestResult(
                success=False,
                message=error_msg
            )
        
        try:
            # Simple connectivity test - just run SELECT 1
            # Use a shorter timeout for testing
            connection = self._get_connection(config, timeout=10)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            
            elapsed = round(float(time.time() - start_time), 2)
            
            return ConnectionTestResult(
                success=True,
                message=f"Connected successfully to Databricks in {elapsed}s",
                details={
                    "host": config.get('host'),
                    "http_path": config.get('http_path'),
                    "response_time_seconds": elapsed
                }
            )
            
        except Exception as e:
            error_msg = str(e)
            
            # User-friendly error messages
            if "Invalid access token" in error_msg or "401" in error_msg or "Unauthorized" in error_msg:
                user_msg = "Authentication failed. Check your Personal Access Token."
            elif "Could not find" in error_msg or "not found" in error_msg.lower():
                user_msg = "Invalid HTTP Path. Check your SQL Warehouse path."
            elif "Network" in error_msg or "timeout" in error_msg.lower() or "getaddrinfo" in error_msg:
                user_msg = f"Cannot reach host '{config.get('host')}'."
            elif "ModuleNotFoundError" in error_msg or "No module named 'databricks'" in error_msg:
                user_msg = "Missing package: databricks-sql-connector. Add to requirements.txt."
            else:
                user_msg = f"Connection failed: {str(error_msg)[:150]}"
            
            return ConnectionTestResult(
                success=False,
                message=user_msg,
                details={"raw_error": str(error_msg)[:500]}
            )
