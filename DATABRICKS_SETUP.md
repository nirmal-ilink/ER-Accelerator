# ER-Accelerator: Databricks Integration Guide

## Quick Start

### 1. Configure Secrets

Edit `.streamlit/secrets.toml` with your Databricks workspace details:

```toml
DATABRICKS_HOST = "https://adb-1234567890123456.16.azuredatabricks.net"
DATABRICKS_CLUSTER_ID = "0123-456789-abcdef12"
DATABRICKS_TOKEN = "your-token-here"
```

**Where to find these values:**
- **DATABRICKS_HOST**: Your workspace URL from browser address bar
- **DATABRICKS_CLUSTER_ID**: Compute → Your Cluster → More (...) → Copy Cluster ID
- **DATABRICKS_TOKEN**: Settings → Developer → Access Tokens → Generate New Token

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Note:** Python 3.12 is required for Databricks Connect compatibility.

### 3. Start the Application

```bash
streamlit run src/frontend/app.py
```

Look for these log messages to confirm Databricks connection:
```
INFO: Initializing Databricks Connect session 'ER_Accelerator_Healthcare'...
INFO: Connecting to: https://your-workspace.azuredatabricks.net
INFO: Cluster ID: 0123-456789-abcdef12
INFO: Successfully connected to Databricks cluster!
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Your Local Machine                        │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Streamlit Frontend (app.py)             │    │
│  │                                                      │    │
│  │   • User Interface                                   │    │
│  │   • Session Management                               │    │
│  │   • Visualization                                    │    │
│  └──────────────────────────┬──────────────────────────┘    │
│                             │                                │
│                    Databricks Connect                        │
│                             │                                │
└─────────────────────────────┼───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Databricks Workspace                        │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Databricks Cluster (DBR 13.3+)          │    │
│  │                                                      │    │
│  │   • Spark Execution                                  │    │
│  │   • Entity Resolution Engine                         │    │
│  │   • Data Processing                                  │    │
│  └──────────────────────────┬──────────────────────────┘    │
│                             │                                │
│  ┌──────────────────────────┴──────────────────────────┐    │
│  │              Unity Catalog                           │    │
│  │                                                      │    │
│  │   • Bronze / Silver / Gold Tables                   │    │
│  │   • Data Governance & Access Control                │    │
│  │   • Volumes for File Storage                        │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Deploying to Databricks Apps (Production)

For production, host the Streamlit app directly in Databricks:

```bash
# Linux/Mac
chmod +x databricks_app_build.sh
./databricks_app_build.sh

# Windows (Git Bash or WSL)
bash databricks_app_build.sh
```

This will:
1. Package your application
2. Create a Secret Scope for credentials
3. Deploy to Databricks Apps

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: databricks.connect` | Run `pip install databricks-connect` |
| `Python version mismatch` | Install Python 3.12 |
| `Cluster not running` | Start your cluster before connecting |
| `Authentication failed` | Regenerate your access token |
| Falls back to local Spark | Check `secrets.toml` has real values (not placeholders) |

## Security Best Practices

1. **Never commit `secrets.toml`** - It's in `.gitignore`
2. **Use Service Principals** for production instead of personal tokens
3. **Rotate tokens regularly** - Azure AD tokens expire automatically
4. **Enable Unity Catalog** - Provides table-level access control
