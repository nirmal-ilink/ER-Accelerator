# Microsoft Fabric Setup Guide

This guide explains how to configure and run the ER-Accelerator on Microsoft Fabric.

## Prerequisites

- Microsoft Fabric workspace with Lakehouse or Warehouse
- Azure CLI installed and authenticated (`az login`)
- ODBC Driver 18 for SQL Server installed
- Python 3.8+ with required dependencies

## Installation

Install the required Python packages:

```bash
pip install pyodbc azure-identity
```

### ODBC Driver Installation

**Windows:**
Download from [Microsoft Downloads](https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server).

**Linux:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y msodbcsql18
```

**macOS:**
```bash
brew install microsoft/mssql-release/msodbcsql18
```

## Authentication

The Fabric connector uses Azure Identity for authentication. It supports:

1. **Azure CLI** (recommended for local dev): Run `az login`
2. **Managed Identity**: Automatic when running in Azure
3. **Service Principal**: Set environment variables:
   ```
   AZURE_CLIENT_ID=<app-id>
   AZURE_CLIENT_SECRET=<secret>
   AZURE_TENANT_ID=<tenant-id>
   ```

## Connection Configuration

When adding a Fabric connector in the UI, you'll need:

| Field | Description | Example |
|-------|-------------|---------|
| **SQL Endpoint** | Fabric SQL analytics endpoint | `xyz.datawarehouse.fabric.microsoft.com` |
| **Database** | Lakehouse or Warehouse name | `MyLakehouse` |

### Finding Your SQL Endpoint

1. Open your Fabric workspace
2. Navigate to your Lakehouse/Warehouse
3. Click **SQL analytics endpoint** in the toolbar
4. Copy the **Server** value from connection strings

## Running the Profiling Notebook

Upload `nb_mdm_profiling_fabric.py` to your Fabric workspace:

1. Go to your Fabric workspace
2. Click **+ New** → **Notebook**
3. Import the `nb_mdm_profiling_fabric.py` file
4. Set these parameters:
   - `schema`: Target schema name
   - `table`: Table to profile
   - `sample_size`: Number of rows to sample
   - `connection_id`: Connection ID from ER-Accelerator

## Troubleshooting

### Authentication Errors
- Ensure you're logged in: `az login`
- Verify Fabric workspace permissions

### ODBC Driver Not Found
- Reinstall ODBC Driver 18
- Check driver is in PATH

### Connection Timeout
- Verify SQL endpoint URL
- Check firewall/network settings

### Permission Denied
- Ensure your Azure identity has access to the Lakehouse/Warehouse
- Check Fabric workspace role assignments
