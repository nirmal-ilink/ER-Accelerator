"""
SQL Server Connector Adapter - Implementation for Microsoft SQL Server.

Handles JDBC connection building, metadata discovery, and connection testing
for SQL Server and Azure SQL Database instances.
"""

import time
from typing import Dict, List, Any
from .base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


class SQLServerAdapter(BaseConnectorAdapter):
    """
    SQL Server adapter for JDBC-based metadata discovery.
    
    Supports:
    - On-premises SQL Server (2016+)
    - Azure SQL Database
    - Azure SQL Managed Instance
    
    Requires the SQL Server JDBC driver on the Spark cluster:
    Maven: com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11
    """
    
    # Connection timeout in seconds
    DEFAULT_TIMEOUT = 30
    DEFAULT_PORT = 1433
    
    @property
    def connector_type(self) -> str:
        return "sqlserver"
    
    @property
    def display_name(self) -> str:
        return "SQL Server"
    
    @property
    def required_fields(self) -> List[str]:
        return ["Server", "Database", "User", "Password"]
    
    def build_jdbc_url(self, config: Dict[str, Any]) -> str:
        """
        Builds SQL Server JDBC URL.
        
        Format: jdbc:sqlserver://server:port;database=db;encrypt=true;...
        
        Args:
            config: Must contain 'server' and 'database'
            
        Returns:
            JDBC URL string
        """
        server = config.get('server', '')
        database = config.get('database', '')
        port = config.get('port', self.DEFAULT_PORT)
        
        # Handle Azure SQL (always requires encryption)
        is_azure = 'database.windows.net' in server.lower()
        encrypt = config.get('encrypt', is_azure)
        trust_cert = config.get('trust_server_certificate', not is_azure)
        
        # Build connection string
        url = f"jdbc:sqlserver://{server}:{port}"
        url += f";databaseName={database}"
        url += f";encrypt={'true' if encrypt else 'false'}"
        url += f";trustServerCertificate={'true' if trust_cert else 'false'}"
        url += f";loginTimeout={config.get('timeout', self.DEFAULT_TIMEOUT)}"
        
        # Connection resilience for Azure
        if is_azure:
            retry_count = config.get('retry_count', 3)
            retry_interval = config.get('retry_interval', 10)
            url += f";connectRetryCount={retry_count};connectRetryInterval={retry_interval}"
        
        return url
    
    def get_jdbc_properties(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Returns JDBC properties for Spark JDBC reader.
        
        Args:
            config: Must contain 'user' and 'password'
            
        Returns:
            Dictionary with authentication and driver properties
        """
        return {
            "user": config.get('user', ''),
            "password": config.get('password', ''),
            "driver": self.get_jdbc_driver_class(),
            # Performance optimizations
            "fetchsize": "1000",
            # Prevent hanging on network issues
            "socketTimeout": str(config.get('timeout', self.DEFAULT_TIMEOUT) * 1000),
        }
    
    def get_jdbc_driver_class(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    
    def fetch_schemas_and_tables(self, spark, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Fetches all schemas and their tables from SQL Server.
        
        Uses INFORMATION_SCHEMA.TABLES for maximum compatibility across
        SQL Server versions and Azure SQL.
        
        Args:
            spark: Active SparkSession
            config: Connection configuration
            
        Returns:
            SchemaMetadata with schema->tables mapping
            
        Raises:
            ConnectionError: If connection fails
            Exception: For other database errors
        """
        start_time = time.time()
        
        # Validate configuration
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            raise ValueError(error_msg)
        
        jdbc_url = self.build_jdbc_url(config)
        jdbc_props = self.get_jdbc_properties(config)
        
        try:
            # Use direct table reference instead of subquery for SQL Server compatibility
            # SQL Server doesn't allow ORDER BY in derived tables
            df = spark.read.jdbc(
                url=jdbc_url,
                table="INFORMATION_SCHEMA.TABLES",
                properties=jdbc_props
            ).filter(
                "TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')"
            ).select("TABLE_SCHEMA", "TABLE_NAME")
            
            # Collect and organize results
            rows = df.collect()
            
            schemas: Dict[str, List[str]] = {}
            for row in rows:
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                if schema_name not in schemas:
                    schemas[schema_name] = []
                schemas[schema_name].append(table_name)
            
            # Sort tables within each schema
            for schema in schemas:
                schemas[schema] = sorted(schemas[schema])
            
            fetch_time = time.time() - start_time
            
            return SchemaMetadata(
                schemas=schemas,
                total_schemas=len(schemas),
                total_tables=sum(len(tables) for tables in schemas.values()),
                fetch_time_seconds=round(fetch_time, 2)
            )
            
        except Exception as e:
            error_msg = str(e)
            
            # Provide user-friendly error messages
            if "Login failed" in error_msg:
                raise ConnectionError(
                    "Authentication failed. Please check your username and password."
                ) from e
            elif "Cannot open database" in error_msg:
                raise ConnectionError(
                    f"Database '{config.get('database')}' not found or access denied."
                ) from e
            elif "Network" in error_msg or "timeout" in error_msg.lower():
                raise ConnectionError(
                    f"Could not connect to server '{config.get('server')}'. "
                    "Please check the server address and ensure it's accessible."
                ) from e
            elif "ClassNotFoundException" in error_msg or "No suitable driver" in error_msg:
                raise ConnectionError(
                    "SQL Server JDBC driver not found on the cluster. "
                    "Please install: com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"
                ) from e
            else:
                raise ConnectionError(f"Connection failed: {error_msg}") from e

    def fetch_columns(self, spark, config: Dict[str, Any], schema: str, table: str) -> List[Dict[str, str]]:
        """
        Fetches column definitions for a specific table from SQL Server.
        
        Args:
            spark: Active SparkSession
            config: Connection configuration
            schema: Schema name
            table: Table name
            
        Returns:
            List of dictionaries containing 'name' and 'type' keys
        """
        jdbc_url = self.build_jdbc_url(config)
        properties = self.get_jdbc_properties(config)
        
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        
        try:
            # Execute query via Spark JDBC
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) as cols",
                properties=properties
            )
            
            rows = df.collect()
            return [{"name": row["COLUMN_NAME"], "type": row["DATA_TYPE"]} for row in rows]
            
        except Exception as e:
            print(f"Error fetching columns for {schema}.{table}: {e}")
            # If query fails, we might return empty list or raise
            raise ConnectionError(f"Failed to fetch columns: {str(e)}")
    
    def test_connection(self, spark, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Tests connection to SQL Server by executing a simple query.
        
        Args:
            spark: Active SparkSession
            config: Connection configuration
            
        Returns:
            ConnectionTestResult with success/failure details
        """
        start_time = time.time()
        
        # Validate configuration first
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            return ConnectionTestResult(
                success=False,
                message=error_msg
            )
        
        # Use a shorter timeout and fewer retries for testing to avoid long hangs
        test_config = config.copy()
        test_config['timeout'] = 10
        test_config['retry_count'] = 1
        test_config['retry_interval'] = 2
        
        jdbc_url = self.build_jdbc_url(test_config)
        jdbc_props = self.get_jdbc_properties(test_config)
        
        # Simple connectivity test query
        test_query = "SELECT 1 AS connection_test, @@VERSION AS version"
        
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({test_query}) AS test",
                properties=jdbc_props
            )
            
            row = df.first()
            elapsed = round(time.time() - start_time, 2)
            
            # Extract SQL Server version info
            version_info = row['version'] if row else "Unknown"
            # Extract first line of version (e.g., "Microsoft SQL Server 2019...")
            version_short = version_info.split('\n')[0][:80] if version_info else "Unknown"
            
            return ConnectionTestResult(
                success=True,
                message=f"Connected successfully in {elapsed}s",
                details={
                    "server": config.get('server'),
                    "database": config.get('database'),
                    "version": version_short,
                    "response_time_seconds": elapsed
                }
            )
            
        except Exception as e:
            error_msg = str(e)
            
            # User-friendly error messages
            if "Login failed" in error_msg:
                user_msg = "Authentication failed. Check username and password."
            elif "Cannot open database" in error_msg:
                user_msg = f"Database '{config.get('database')}' not found."
            elif "Network" in error_msg or "timeout" in error_msg.lower():
                user_msg = f"Cannot reach server '{config.get('server')}'."
            elif "ClassNotFoundException" in error_msg:
                user_msg = "JDBC driver not installed on cluster."
            elif "[NO_ACTIVE_SESSION]" in error_msg:
                user_msg = "Databricks Cluster is not reachable or Spark Session is inactive. Since you are running locally with Databricks Connect, Spark operations require an active cluster. Please ensure your Databricks cluster is running and accessible."
            else:
                user_msg = f"Connection failed: {error_msg[:100]}"
            
            return ConnectionTestResult(
                success=False,
                message=user_msg,
                details={"raw_error": error_msg[:500]}
            )
