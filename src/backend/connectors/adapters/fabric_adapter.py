"""
Microsoft Fabric Connector Adapter - Implementation for Fabric Lakehouse/Warehouse.

Uses pyodbc with Azure Identity for TDS connection to Fabric SQL endpoints.
Supports OneLake Lakehouse and Warehouse metadata discovery.
"""

import time
from typing import Dict, List, Any, Optional
from .base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


class FabricAdapter(BaseConnectorAdapter):
    """
    Microsoft Fabric adapter for Lakehouse/Warehouse metadata discovery.
    
    Uses pyodbc with ODBC Driver 18 for SQL Server to connect via TDS protocol.
    Authentication via Azure Identity (Service Principal or Interactive).
    
    Supports:
    - Fabric Lakehouse SQL Endpoint
    - Fabric Warehouse
    
    Requires: 
    - pip install pyodbc azure-identity
    - ODBC Driver 18 for SQL Server installed
    """
    
    # Connection timeout in seconds
    DEFAULT_TIMEOUT = 30
    
    @property
    def connector_type(self) -> str:
        return "fabric"
    
    @property
    def display_name(self) -> str:
        return "Microsoft Fabric"
    
    @property
    def required_fields(self) -> List[str]:
        return ["SQL Endpoint", "Database"]
    
    @property
    def requires_spark_for_test(self) -> bool:
        """Fabric uses pyodbc directly, no Spark needed for connection test."""
        return False
    
    def _get_connection(self, config: Dict[str, Any], timeout: int = 30):
        """
        Create a connection to Fabric SQL Endpoint using pyodbc.
        
        Uses DefaultAzureCredential for authentication which supports:
        - Azure CLI login (for local dev)
        - Managed Identity (for Azure-hosted apps)
        - Service Principal (via environment variables)
        
        Args:
            config: Connection configuration
            timeout: Socket timeout in seconds
            
        Returns:
            pyodbc Connection object
        """
        import pyodbc
        from azure.identity import DefaultAzureCredential
        
        sql_endpoint = config.get('sql_endpoint', '').strip()
        database = config.get('database', '').strip()
        
        # Clean up endpoint URL if needed
        if sql_endpoint.startswith('https://'):
            sql_endpoint = sql_endpoint[8:]
        if sql_endpoint.startswith('http://'):
            sql_endpoint = sql_endpoint[7:]
        # Remove trailing path if present
        if '/' in sql_endpoint:
            sql_endpoint = sql_endpoint.split('/')[0]
        
        # Get access token using Azure Identity
        credential = DefaultAzureCredential()
        # Fabric uses the Azure SQL Database scope
        token = credential.get_token("https://database.windows.net/.default")
        
        # Build connection string for Fabric
        # Using ActiveDirectoryAccessToken authentication
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={sql_endpoint};"
            f"Database={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout={timeout};"
        )
        
        # Use access token for authentication
        # pyodbc requires the token as a struct for Azure AD token auth
        import struct
        token_bytes = token.token.encode('utf-16-le')
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        
        # SQL_COPT_SS_ACCESS_TOKEN = 1256
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        
        return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    def build_jdbc_url(self, config: Dict[str, Any]) -> str:
        """Not used for Fabric - kept for interface compatibility."""
        return ""
    
    def get_jdbc_properties(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Not used for Fabric - kept for interface compatibility."""
        return {}
    
    def get_jdbc_driver_class(self) -> str:
        """Not used for Fabric - kept for interface compatibility."""
        return ""
    
    def fetch_schemas_and_tables(self, spark, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Fetch all schemas and their tables from the Fabric Lakehouse/Warehouse.
        
        Args:
            spark: SparkSession (not used, kept for interface compatibility)
            config: Connection configuration
            
        Returns:
            SchemaMetadata with schema->tables mapping
            
        Raises:
            ConnectionError: If connection fails
        """
        start_time = time.time()
        
        try:
            connection = self._get_connection(config)
            cursor = connection.cursor()
            
            # Get all schemas (excluding system schemas)
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
                ORDER BY schema_name
            """)
            schema_rows = cursor.fetchall()
            
            schemas: Dict[str, List[str]] = {}
            
            for schema_row in schema_rows:
                schema_name = schema_row[0]
                
                # Get tables for this schema
                cursor.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = ?
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (schema_name,))
                
                table_rows = cursor.fetchall()
                tables = [row[0] for row in table_rows]
                
                if tables:
                    schemas[schema_name] = tables
            
            cursor.close()
            connection.close()
            
            fetch_time = time.time() - start_time
            
            return SchemaMetadata(
                schemas=schemas,
                total_schemas=len(schemas),
                total_tables=sum(len(tables) for tables in schemas.values()),
                fetch_time_seconds=round(fetch_time, 2)
            )
            
        except Exception as e:
            error_msg = str(e)
            
            if "Login failed" in error_msg or "authentication" in error_msg.lower():
                raise ConnectionError(
                    "Authentication failed. Please ensure you're logged in with Azure CLI "
                    "or have valid Azure credentials configured."
                ) from e
            elif "network" in error_msg.lower() or "timeout" in error_msg.lower():
                raise ConnectionError(
                    f"Could not connect to '{config.get('sql_endpoint')}'. "
                    "Please check the SQL Endpoint URL."
                ) from e
            elif "ODBC Driver" in error_msg:
                raise ConnectionError(
                    "ODBC Driver 18 for SQL Server is not installed. "
                    "Please install it from Microsoft's website."
                ) from e
            else:
                raise ConnectionError(f"Connection failed: {error_msg}") from e
    
    def fetch_columns(self, spark, config: Dict[str, Any], schema: str, table: str) -> List[Dict[str, str]]:
        """
        Fetch column definitions for a specific table from Fabric.
        
        Args:
            spark: SparkSession (not used)
            config: Connection configuration
            schema: Schema name
            table: Table name
            
        Returns:
            List of dictionaries containing 'name' and 'type' keys
        """
        try:
            connection = self._get_connection(config)
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, (schema, table))
            
            rows = cursor.fetchall()
            columns = [{"name": row[0], "type": row[1]} for row in rows]
            
            cursor.close()
            connection.close()
            
            return columns
            
        except Exception as e:
            print(f"Error fetching columns for {schema}.{table}: {e}")
            raise ConnectionError(f"Failed to fetch columns: {str(e)}")
    
    def test_connection(self, spark, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Test connection to Fabric SQL Endpoint with a simple query.
        
        Args:
            spark: SparkSession (not used)
            config: Connection configuration
            
        Returns:
            ConnectionTestResult with success/failure details
        """
        start_time = time.time()
        
        # Validate required fields
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            return ConnectionTestResult(
                success=False,
                message=error_msg
            )
        
        try:
            connection = self._get_connection(config, timeout=15)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            
            elapsed = round(time.time() - start_time, 2)
            
            return ConnectionTestResult(
                success=True,
                message=f"Connected successfully to Microsoft Fabric in {elapsed}s",
                details={
                    "sql_endpoint": config.get('sql_endpoint'),
                    "database": config.get('database'),
                    "response_time_seconds": elapsed
                }
            )
            
        except ImportError as e:
            return ConnectionTestResult(
                success=False,
                message="Missing package: pyodbc or azure-identity. Please install them.",
                details={"raw_error": str(e)}
            )
        except Exception as e:
            error_msg = str(e)
            
            # User-friendly error messages
            if "Login failed" in error_msg or "authentication" in error_msg.lower():
                user_msg = "Authentication failed. Run 'az login' to authenticate with Azure CLI."
            elif "ODBC Driver" in error_msg:
                user_msg = "ODBC Driver 18 for SQL Server is not installed."
            elif "network" in error_msg.lower() or "timeout" in error_msg.lower():
                user_msg = f"Cannot reach endpoint '{config.get('sql_endpoint')}'."
            else:
                user_msg = f"Connection failed: {error_msg[:150]}"
            
            return ConnectionTestResult(
                success=False,
                message=user_msg,
                details={"raw_error": error_msg[:500]}
            )
