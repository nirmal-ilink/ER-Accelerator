import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.backend.connectors.connector_service import get_connector_service
    from src.backend.utils.git_utils import get_current_branch
    import json
    import uuid

    print("initializing connector service...")
    service = get_connector_service()
    
    branch = get_current_branch()
    is_fabric = service._is_fabric_mode()
    
    print(f"------------ MODE CHECK ------------")
    print(f"Current Branch: {branch}")
    print(f"Connector Mode: {'[FABRIC]' if is_fabric else '[DATABRICKS]'}")
    print(f"------------------------------------")
    
    if is_fabric:
        print("[OK] Running in FABRIC mode.")
        print("Operations will read/write to Fabric SQL Endpoint.")
    else:
        print("[OK] Running in DATABRICKS mode.")
        print("Operations will read/write to Databricks Delta Tables.")

    # Test Data
    connection_id = str(uuid.uuid4())
    test_record = {
        "source_type": "fabric",
        "source_name": f"Test {branch} Connection",
        "configuration": json.dumps({"test": "true"}),
        "selected_tables": json.dumps({"dbo": ["test_table"]}),
        "load_type": "full",
        "watermark_column": None,
        "schedule_enabled": False,
        "schedule_cron": None,
        "schedule_timezone": "UTC",
        "status": "active"
    }
    
    # --- Debugging Connection ---
    print("\n[DEBUG] Starting detailed connectivity test...")
    import struct
    import pyodbc
    import subprocess
    from azure.identity import AzureCliCredential
    
    # Check current Azure CLI identity
    try:
        print("[DEBUG] Checking current Azure CLI account...")
        az_output = subprocess.check_output(["az", "account", "show", "--query", "user.name", "-o", "tsv"], shell=True).decode().strip()
        print(f"[INFO] Current Azure User: {az_output}")
    except Exception as e:
        print(f"[WARN] Could not detect Azure CLI user: {e}")

    # Force CLI Credential to ensure we use the logged-in user
    print("[DEBUG] Acquiring token using AzureCliCredential...")
    try:
        # Increase timeout to 30 seconds (default is often 10)
        credential = AzureCliCredential(process_timeout=30)
        token = credential.get_token("https://database.windows.net/.default")
        print(f"[DEBUG] Token acquired! (Len: {len(token.token)})")
    except Exception as e:
        print(f"[ERROR] Failed to acquire token: {e}")
        # Don't raise immediately, let the Interactive auth try if this fails
        # raise e

    driver_name = "{ODBC Driver 18 for SQL Server}"
    available_drivers = pyodbc.drivers()
    print(f"[DEBUG] Available ODBC Drivers: {available_drivers}")
    
    if "ODBC Driver 18 for SQL Server" not in available_drivers:
        print("[WARN] 'ODBC Driver 18 for SQL Server' not found. Trying 17 or 13...")
        if "ODBC Driver 17 for SQL Server" in available_drivers:
            driver_name = "{ODBC Driver 17 for SQL Server}"
        elif "ODBC Driver 13 for SQL Server" in available_drivers:
            driver_name = "{ODBC Driver 13 for SQL Server}"
        else:
            print("[ERROR] No suitable SQL Server ODBC Driver found!")

    # Connection Details
    server = "ohk6lkhiim6ezfv6gravnt3iq4-qjn3af3jrkje7ellmgoj635c7q.datawarehouse.fabric.microsoft.com"
    database = "wh_mdm" 
    
    token_bytes = token.token.encode('utf-16-le')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    
    # ATTEMPT 1: Target DB, Strict Certs
    print(f"\n[DEBUG] --- ATTEMPT 1: Target DB '{database}' ---")
    conn_str = (
        f"Driver={driver_name};"
        f"Server={server},1433;"
        f"Database={database};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;" 
        f"Connection Timeout=30;"
    )
    
    conn = None
    try:
        conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        print("[OK] Connection Successful to Target DB!")
    except Exception as e:
        print(f"[ERROR] Attempt 1 Failed: {e}")
        
        # ATTEMPT 2: Master DB, Loose Certs (Diagnostics)
        print(f"\n[DEBUG] --- ATTEMPT 2: Master DB + TrustServerCertificate=yes ---")
        conn_str_2 = (
            f"Driver={driver_name};"
            f"Server={server},1433;"
            f"Database=master;" # Try master to check server access
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;" # Relax certs
            f"Connection Timeout=15;"
        )
        try:
            conn = pyodbc.connect(conn_str_2, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            print("[OK] Connection Successful to MASTER DB!")
            print("[WARN] Issue likely related to 'lh_mdm' database permissions or existence.")
        except Exception as e2:
            print(f"[ERROR] Attempt 2 Failed: {e2}")
            
            # ATTEMPT 3: ActiveDirectoryInteractive (Driver handles auth)
            print(f"\n[DEBUG] --- ATTEMPT 3: Authentication=ActiveDirectoryInteractive ---")
            print("[INFO] This might open a browser window for login...")
            conn_str_3 = (
                f"Driver={driver_name};"
                f"Server={server},1433;"
                f"Database={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Authentication=ActiveDirectoryInteractive;"
            )
            try:
                # We don't pass the token here, letting the driver handle it
                conn = pyodbc.connect(conn_str_3)
                print("[OK] Connection Successful via Interactive Auth!")
                print("[INFO] RECOMMENDATION: The Service Principal/CLI token methodology is failing, but Interactive works.")
                print("       You may need to update the app to use interactive auth or fix the CLI token scope.")
            except Exception as e3:
                print(f"[ERROR] Attempt 3 Failed: {e3}")
                print("CRITICAL: All connection methods failed.")
                raise e3

    if conn:
        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            print(f"   Server Version: {row[0]}")


    # Only if connection passed, try the service logic
    if is_fabric:
        print("\n[DEBUG] Proceeding with Service-level test...")
        
        # Mocking secrets for the save call as we are running standalone
        from unittest.mock import MagicMock
        # Mock private attribute instead of property
        mock_sm = MagicMock()
        mock_sm.put_secret = MagicMock()
        mock_sm.get_secret_metadata_pointer = lambda k: f"secret://{k}"
        service._secret_manager = mock_sm
        
        # Restore mock for service call validation
        service.save_configuration(
            connector_type=test_record["source_type"],
            connector_name=test_record["source_name"],
            config={"test": "true"},
            selected_tables={"dbo": ["test_table"]}
        )
        print("[OK] Successfully saved test record via Service!")
        
        # Verify Read
        print("Verifying read back...")
        loaded = service.get_configuration_by_id(connection_id)
        if loaded:
            print(f"[OK] Successfully loaded config: {loaded.connector_name}")
        else:
            print("[ERROR] Failed to load config back.")


except ImportError as e:
    print(f"[ERROR] Import Error: {e}")
except Exception as e:
    print(f"[ERROR] Verification Failed: {e}")
    import traceback
    traceback.print_exc()
