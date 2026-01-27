import csv
import random
from datetime import datetime, timedelta
import os
import sys

# Add project root to path to ensure we can import if needed (though this script is standalone mostly)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def generate_audit_logs(num_records=500):
    # Setup paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    data_dir = os.path.join(project_root, "data")
    log_path = os.path.join(data_dir, "audit_log.csv")
    
    os.makedirs(data_dir, exist_ok=True)
    
    # Data definitions
    vendors = ["Salesforce", "SAP", "Snowflake", "Databricks", "Oracle", "AWS Redshift", "Azure Synapse", "Google BigQuery"]
    modules = ["Connector", "Profiler", "Inspector", "Governance", "Catalogue", "Quality", "Lineage"]
    
    actions_map = {
        "Connector": ["Schema Scan", "Connection Test", "Metadata Sync", "Incremental Load"],
        "Profiler": ["Column Profile", "Data Quality Check", "Pattern Analysis", "Null Check"],
        "Inspector": ["PII Detection", "Sensitive Data Scan", "Compliance Check"],
        "Governance": ["Access Control Review", "Policy Update", "Tag Assignment"],
        "Catalogue": ["Asset Tagging", "Description Update", "Glossary Link"],
        "Quality": ["Rule validation", "Anomaly Detection"],
        "Lineage": ["Graph Build", "Dependency Check"]
    }
    
    statuses = ["Success", "Success", "Success", "Warning", "Failed"] # Weighted towards success
    
    users = ["System", "admin@icore.com", "j.doe@client.com", "etl_service_account", "m.smith@icore.com", "data_steward_1"]
    
    records = []
    
    # Generate records over last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"Generating {num_records} records...")
    
    for _ in range(num_records):
        # Random timestamp
        random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
        timestamp = start_date + timedelta(seconds=random_seconds)
        
        module = random.choice(modules)
        action = random.choice(actions_map.get(module, ["Generic Action"]))
        vendor = random.choice(vendors)
        status = random.choice(statuses)
        user = random.choice(users)
        
        # Details depend on context
        if status == "Failed":
             details = f"Connection timeout to {vendor} instance (Error 504)"
        elif status == "Warning":
             details = f"Scan completed with 3 warnings on {vendor} tables"
        else:
             details = f"Successfully processed {random.randint(10, 500)} objects from {vendor}"
             
        records.append([
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            user,
            action,
            module,
            status,
            details
        ])
    
    # Sort by timestamp
    records.sort(key=lambda x: x[0], reverse=True)
    
    # Write to CSV
    headers = ["Timestamp", "User", "Action", "Module", "Status", "Details"]
    
    with open(log_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)
        
    print(f"Successfully wrote {len(records)} audit logs to {log_path}")

if __name__ == "__main__":
    generate_audit_logs()
