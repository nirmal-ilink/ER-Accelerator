# Connector services package
"""
Backend services for data source connectors.
Provides abstraction layer for metadata discovery and configuration management.
"""

from .connector_service import ConnectorService, get_connector_service

__all__ = ["ConnectorService", "get_connector_service"]
