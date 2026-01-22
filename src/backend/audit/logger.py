import os
import csv
from datetime import datetime
import json

class AuditLogger:
    """
    Centralized System Logger.
    Writes structured events to a local CSV file for observability and audit trails.
    """
    
    def __init__(self, spark=None, log_path: str = None):
        # spark argument is kept for backward compatibility but ignored
        
        # Default to a data directory relative to the project root if not specified
        if not log_path:
             # Assuming this file is in src/backend/audit/logger.py
             # Go up 3 levels to root, then into data
             base_dir = os.path.dirname(os.path.abspath(__file__))
             project_root = os.path.abspath(os.path.join(base_dir, "../../../"))
             self.log_path = os.path.join(project_root, "data", "audit_log.csv")
        else:
            # If log_path passed from legacy code is a directory, append filename
            if not log_path.endswith('.csv'):
                 self.log_path = os.path.join(log_path, "audit_log.csv")
            else:
                 self.log_path = log_path
            
        self._ensure_log_file()

    def _ensure_log_file(self):
        """Creates the audit CSV file with headers if it doesn't exist."""
        try:
             # Ensure directory exists
             os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
             
             if not os.path.exists(self.log_path):
                 headers = ["Timestamp", "User", "Action", "Module", "Status", "Details"]
                 with open(self.log_path, 'w', newline='', encoding='utf-8') as f:
                     writer = csv.writer(f)
                     writer.writerow(headers)
        except Exception as e:
             print(f"ERROR: Failed to initialize audit log file: {e}")

    def log_event(self, module: str, action: str, status: str, details: any = None, user: str = "SYSTEM"):
        """
        Appends an event to the log CSV.
        Signature adapted to be compatible with legacy calls:
        (stage/module, action, status, details, user)
        """
        
        # Handle details being a dict
        if isinstance(details, dict):
            details_str = json.dumps(details)
        else:
            details_str = str(details) if details else ""

        event = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user,
            action,
            module,
            status,
            details_str
        ]
        
        try:
            with open(self.log_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(event)
            print(f"AUDIT LOG: [{module}] {action} - {status} : {details_str}")
        except Exception as e:
            print(f"ERROR: Failed to write to audit log: {e}")
