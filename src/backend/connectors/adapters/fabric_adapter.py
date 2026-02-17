"""
Fabric Connector Adapter - Implementation for Microsoft Fabric Data Warehouse.

Handles JDBC connection building, metadata discovery, and connection testing
for Microsoft Fabric Data Warehouse using SQL Server JDBC driver.
"""

import time
from typing import Dict, List, Any
from .base_adapter import BaseConnectorAdapter, ConnectionTestResult, SchemaMetadata


class FabricAdapter(BaseConnectorAdapter):
    """
    Microsoft Fabric adapter for JDBC-based metadata discovery.
    
    Uses standard SQL Server JDBC driver but with specific configuration
    for Fabric Data Warehouse (Service Principal Auth).
    """
    
    # Connection timeout in seconds
    DEFAULT_TIMEOUT = 30
    DEFAULT_PORT = 1433
    
    @property
    def connector_type(self) -> str:
        return "fabric"
    
    @property
    def display_name(self) -> str:
        return "Microsoft Fabric"
    
    @property
    def required_fields(self) -> List[str]:
        # User requested specific fields
        return ["Tenant ID", "Client ID", "Workspace ID", "Client Secret", "Table Name"]
    
    def fetch_catalogs(self, spark, config: Dict[str, Any]) -> List[str]:
        """
        Fetches available databases/warehouses from Fabric.
        
        Args:
            spark: Active SparkSession
            config: Connection configuration
            
        Returns:
            List of database names
        """
        # Validate configuration
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            raise ValueError(error_msg)
        
        jdbc_url = self.build_jdbc_url(config)
        jdbc_props = self.get_jdbc_properties(config)
        
        try:
            # Query system catalog for databases
            query = """
            SELECT name 
            FROM master.sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
            ORDER BY name
            """
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) as dbs",
                properties=jdbc_props
            )
            
            rows = df.collect()
            return [row['name'] for row in rows]
            
        except Exception as e:
            print(f"Error fetching Fabric databases: {e}")
            return []
            
    def build_jdbc_url(self, config: Dict[str, Any]) -> str:
        """
        Builds Fabric (SQL Server) JDBC URL.
        
        Args:
            config: Must contain 'Workspace ID' (treated as server/host mostly)
            
        Returns:
            JDBC URL string
        """
        # Mapping user fields to JDBC params
        # The user provides 'Workspace ID', but technically we need the SQL Endpoint.
        # Assuming Workspace ID might be the full SQL Endpoint or we construct it.
        # usually: <server>.datawarehouse.fabric.microsoft.com
        
        workspace_id = config.get('workspace_id', '').strip()
        
        # Heuristic: if it doesn't look like a full URL, maybe it's just the ID?
        # But for now, we'll treat the input "Workspace ID" as the Server Address 
        # because the user might paste the entire endpoint string there.
        # If it is just a UUID, this might fail, but we don't have the format logic yet.
        server = workspace_id 
        
        # We don't have a specific 'Database' field in the requested list,
        # but JDBC needs one. We'll default to 'master' or try to infer.
        # If the user enters a full connection string or similar, we might need to parse.
        # For now, let's assume valid SQL connection needs a DB. 
        # We'll use master if not specified, but Fabric DW usually requires connecting to the specific item.
        # Since 'Table Name' is provided, maybe 'table_name' implies a specific DB context? Unlikely.
        
        # Let's try connecting to the server root (master) or just correct syntax.
        # If the user *meant* "Server" when they said "Workspace ID".
        
        port = self.DEFAULT_PORT
        database = "master" # Default to master if not derivable
        
        url = f"jdbc:sqlserver://{server}:{port}"
        url += f";databaseName={database}"
        url += ";encrypt=true"
        url += ";trustServerCertificate=false"
        url += f";loginTimeout={self.DEFAULT_TIMEOUT}"
        
        return url
    
    def get_jdbc_properties(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Returns JDBC properties for Spark JDBC reader.
        Uses ActiveDirectoryServicePrincipal for authentication.
        """
        tenant_id = config.get('tenant_id', '').strip()
        client_id = config.get('client_id', '').strip()
        client_secret = config.get('client_secret', '').strip()
        
        return {
            "driver": self.get_jdbc_driver_class(),
            "authentication": "ActiveDirectoryServicePrincipal",
            "userName": client_id,
            "password": client_secret,
            "aadSecurePrincipalId": client_id,
            "aadSecurePrincipalSecret": client_secret,
            # "encrypt": "true", # Already in URL
            # "hostNameInCertificate": "*.datawarehouse.fabric.microsoft.com" # Optional check
        }
    
    def get_jdbc_driver_class(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    
    def fetch_schemas_and_tables(self, spark, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Fetches schemas and tables. 
        If 'Table Name' is specified in config, we might restrict discovery,
        but typically this method returns *all* available for selection.
        """
        start_time = time.time()
        
        # Validate configuration
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            raise ValueError(error_msg)
        
        jdbc_url = self.build_jdbc_url(config)
        jdbc_props = self.get_jdbc_properties(config)
        
        try:
            # We filter by the provided table name if available?
            # User demanded 'table_name' as input. 
            target_table = config.get('table_name', '').strip()
            
            query = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            if target_table:
                query += f" AND TABLE_NAME = '{target_table}'"
                
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) as t",
                properties=jdbc_props
            )
            
            rows = df.collect()
            
            schemas: Dict[str, List[str]] = {}
            for row in rows:
                schema = row['TABLE_SCHEMA']
                table = row['TABLE_NAME']
                if schema not in schemas:
                    schemas[schema] = []
                schemas[schema].append(table)
                
            # Sort
            for s in schemas:
                schemas[s] = sorted(schemas[s])
                
            fetch_time = time.time() - start_time
            
            return SchemaMetadata(
                schemas=schemas,
                total_schemas=len(schemas),
                total_tables=sum(len(t) for t in schemas.values()),
                fetch_time_seconds=round(float(fetch_time), 2)
            )
            
        except Exception as e:
            # Error handling similar to SQLServerAdapter
            raise ConnectionError(f"Fabric connection failed: {e}") from e

    def fetch_columns(self, spark, config: Dict[str, Any], schema: str, table: str) -> List[Dict[str, str]]:
        """
        Fetches column definitions.
        """
        jdbc_url = self.build_jdbc_url(config)
        properties = self.get_jdbc_properties(config)
        
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table="INFORMATION_SCHEMA.COLUMNS",
                properties=properties
            ).filter(
                f"TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
            ).select("COLUMN_NAME", "DATA_TYPE", "ORDINAL_POSITION")
            
            rows = df.collect()
            sorted_rows = sorted(rows, key=lambda r: r["ORDINAL_POSITION"])
            return [{"name": row["COLUMN_NAME"], "type": row["DATA_TYPE"]} for row in sorted_rows]
            
        except Exception as e:
            raise ConnectionError(f"Failed to fetch columns: {str(e)}")

    def test_connection(self, spark, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Tests connection to Fabric.
        """
        start_time = time.time()
        
        is_valid, error_msg = self.validate_config(config)
        if not is_valid:
            return ConnectionTestResult(success=False, message=error_msg)
            
        jdbc_url = self.build_jdbc_url(config)
        jdbc_props = self.get_jdbc_properties(config)
        
        try:
            # Simple test query
            test_query = "SELECT 1 AS connection_test, @@VERSION AS version"
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({test_query}) AS test",
                properties=jdbc_props
            )
            
            row = df.first()
            elapsed = round(float(time.time() - start_time), 2)
            version_info = row['version'] if row else "Unknown"
            
            return ConnectionTestResult(
                success=True,
                message=f"Connected to Fabric successfully in {elapsed}s",
                details={
                    "version": version_info[:50],
                    "response_time": elapsed
                }
            )
            
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {str(e)[:200]}",
                details={"raw_error": str(e)}
            )
