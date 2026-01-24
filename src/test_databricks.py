"""
Databricks Connection Test Script (Windows Compatible)
Run this to verify that your credentials and connection work.
"""
import os
import sys

def test_databricks_connection():
    print("=" * 60)
    print("DATABRICKS CONNECTION TEST")
    print("=" * 60)
    
    # Step 1: Check credentials
    print("\n[1/3] Checking credentials...")
    
    try:
        import toml
        secrets_path = os.path.join(os.path.dirname(__file__), '../.streamlit/secrets.toml')
        if os.path.exists(secrets_path):
            secrets = toml.load(secrets_path)
            host = secrets.get('DATABRICKS_HOST', 'NOT SET')
            cluster_id = secrets.get('DATABRICKS_CLUSTER_ID', 'NOT SET')
            token = secrets.get('DATABRICKS_TOKEN', '')
            token_preview = token[:20] + '...' if token else 'NOT SET'
            
            print(f"   Host: {host}")
            print(f"   Cluster ID: {cluster_id}")
            print(f"   Token: {token_preview}")
            
            if 'YOUR_' in host or 'YOUR_' in cluster_id:
                print("   [FAIL] Credentials contain placeholder values!")
                return False
            print("   [OK] Credentials loaded from secrets.toml")
        else:
            print("   [FAIL] secrets.toml not found!")
            return False
    except Exception as e:
        print(f"   [FAIL] Error loading secrets: {e}")
        return False
    
    # Step 2: Test Databricks SDK connection
    print("\n[2/3] Testing Databricks SDK connection...")
    try:
        from databricks.sdk import WorkspaceClient
        
        # Initialize client with credentials
        w = WorkspaceClient(
            host=host,
            token=token
        )
        
        # Get current user to verify connection
        me = w.current_user.me()
        print(f"   [OK] Connected as: {me.user_name}")
        print(f"   [OK] Display name: {me.display_name}")
    except Exception as e:
        print(f"   [FAIL] SDK connection failed: {e}")
        return False
    
    # Step 3: Verify cluster exists and get status
    print("\n[3/3] Checking cluster status...")
    try:
        cluster = w.clusters.get(cluster_id)
        print(f"   [OK] Cluster found: {cluster.cluster_name}")
        print(f"   [OK] State: {cluster.state}")
        print(f"   [OK] Spark Version: {cluster.spark_version}")
        
        if str(cluster.state) == "State.RUNNING":
            print("\n" + "=" * 60)
            print("SUCCESS! Databricks connection verified.")
            print("=" * 60)
            print(f"\nCluster '{cluster.cluster_name}' is RUNNING")
            print("Your Streamlit app can now execute Spark jobs on Databricks!")
        elif str(cluster.state) == "State.TERMINATED":
            print("\n" + "=" * 60)
            print("CLUSTER IS STOPPED")
            print("=" * 60)
            print(f"\nCluster '{cluster.cluster_name}' is terminated.")
            print("Start it from the Databricks UI, then run your app.")
        else:
            print(f"\nCluster state: {cluster.state}")
            print("Wait for cluster to be RUNNING before using the app.")
        
        return True
        
    except Exception as e:
        print(f"   [FAIL] Could not get cluster: {e}")
        return False


if __name__ == "__main__":
    try:
        import toml
    except ImportError:
        print("Installing toml package...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "toml", "-q"])
    
    success = test_databricks_connection()
    sys.exit(0 if success else 1)
