# Connector adapters package
"""
Database-specific adapter implementations.
Each adapter handles JDBC connection building and metadata queries for its database type.
"""

from .base_adapter import BaseConnectorAdapter
from .sql_server_adapter import SQLServerAdapter
from .databricks_adapter import DatabricksAdapter

__all__ = ["BaseConnectorAdapter", "SQLServerAdapter", "DatabricksAdapter"]
