"""
Base Connector Adapter - Abstract base class for all database connector adapters.

This module defines the contract that all database-specific adapters must implement.
Following the Strategy pattern, each adapter encapsulates database-specific logic
for connection building, metadata discovery, and connection testing.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class ConnectionTestResult:
    """Result of a connection test attempt."""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class SchemaMetadata:
    """Metadata for a database schema/table structure."""
    schemas: Dict[str, List[str]]  # schema_name -> [table_names]
    total_schemas: int
    total_tables: int
    fetch_time_seconds: float


class BaseConnectorAdapter(ABC):
    """
    Abstract base class for database connector adapters.
    
    Each database type (SQL Server, Snowflake, Oracle, etc.) should have
    a concrete implementation of this class that handles database-specific
    nuances in connection strings, authentication, and metadata queries.
    """
    
    @property
    @abstractmethod
    def connector_type(self) -> str:
        """
        Returns the unique identifier for this connector type.
        Example: 'sqlserver', 'snowflake', 'oracle'
        """
        pass
    
    @property
    @abstractmethod
    def display_name(self) -> str:
        """
        Returns the human-readable name for this connector.
        Example: 'SQL Server', 'Snowflake', 'Oracle DB'
        """
        pass
    
    @property
    @abstractmethod
    def required_fields(self) -> List[str]:
        """
        Returns list of required configuration fields.
        Example: ['server', 'database', 'user', 'password']
        """
        pass
    
    @property
    def requires_spark_for_test(self) -> bool:
        """
        Indicates if this adapter requires a Spark session for connectivity testing.
        Most JDBC-based adapters do, while REST or CLI based ones might not.
        """
        return True
    
    @abstractmethod
    def build_jdbc_url(self, config: Dict[str, Any]) -> str:
        """
        Builds the JDBC connection URL from configuration.
        
        Args:
            config: Dictionary containing connection parameters
            
        Returns:
            JDBC URL string (e.g., 'jdbc:sqlserver://server:1433;database=db')
        """
        pass
    
    @abstractmethod
    def get_jdbc_properties(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Returns JDBC connection properties (user, password, driver options).
        
        Args:
            config: Dictionary containing connection parameters
            
        Returns:
            Dictionary of JDBC properties for Spark JDBC reader
        """
        pass
    
    @abstractmethod
    def get_jdbc_driver_class(self) -> str:
        """
        Returns the fully qualified JDBC driver class name.
        Example: 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        """
        pass
    
    @abstractmethod
    def fetch_schemas_and_tables(self, spark, config: Dict[str, Any]) -> SchemaMetadata:
        """
        Connects to the database and retrieves all schemas and their tables.
        
        Args:
            spark: Active SparkSession (local or Databricks Connect)
            config: Dictionary containing connection parameters
            
        Returns:
            SchemaMetadata object with schema->tables mapping
            
        Raises:
            ConnectionError: If unable to connect to database
            PermissionError: If user lacks read access to metadata
        """
        pass
    
    @abstractmethod
    def test_connection(self, spark, config: Dict[str, Any]) -> ConnectionTestResult:
        """
        Tests the connection to the database.
        
        Args:
            spark: Active SparkSession
            config: Dictionary containing connection parameters
            
        Returns:
            ConnectionTestResult with success status and message
        """
        pass

    @abstractmethod
    def fetch_columns(self, spark, config: Dict[str, Any], schema: str, table: str) -> List[Dict[str, str]]:
        """
        Fetches column definitions for a specific table.
        
        Args:
            spark: Active SparkSession
            config: Connection configuration
            schema: Schema name
            table: Table name
            
        Returns:
            List of dictionaries containing 'name' and 'type' keys
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> tuple[bool, str]:
        """
        Validates that all required fields are present and non-empty.
        
        Args:
            config: Dictionary containing connection parameters
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        missing_fields = []
        for field in self.required_fields:
            field_key = field.lower().replace(' ', '_')
            if field_key not in config or not config[field_key]:
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        
        return True, "Configuration is valid"
    
    def get_connection_info(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Returns sanitized connection info for display (no secrets).
        
        Args:
            config: Dictionary containing connection parameters
            
        Returns:
            Dictionary with safe-to-display connection details
        """
        safe_config = {}
        secret_keywords = ['password', 'token', 'key', 'secret']
        
        for key, value in config.items():
            if any(kw in key.lower() for kw in secret_keywords):
                safe_config[key] = '********'
            else:
                safe_config[key] = value
        
        return safe_config
