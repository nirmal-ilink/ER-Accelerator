import csv
import os
import pandas as pd
from datetime import datetime

class ResolutionManager:
    def __init__(self, output_path="data/output/resolutions.csv"):
        self.output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../", output_path))
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """Create the CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.output_path):
            with open(self.output_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "cluster_id", "action", "user", "comments", "resolved_data"])

    def log_resolution(self, cluster_id, action, user, comments="", resolved_data=None):
        """Append a resolution decision to the CSV."""
        try:
            import json
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            resolved_json = json.dumps(resolved_data) if resolved_data else ""
            with open(self.output_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, cluster_id, action, user, comments, resolved_json])
            return True
        except Exception as e:
            print(f"Error logging resolution: {e}")
            return False

    def get_resolved_ids(self):
        """Return a set of cluster_ids that have already been resolved (Approved or Rejected)."""
        if not os.path.exists(self.output_path):
            return set()
        
        try:
            df = pd.read_csv(self.output_path)
            # We consider 'Approved' and 'Rejected' as final states. 'Deferred' might come back.
            # For now, let's treat any entry as 'viewed/handled' but user asked to persist resolutions.
            # Typically you'd filter for non-deferred if you want deferred to show up again.
            # Let's assume Deferred puts it back in queue, so we only exclude Approved/Rejected.
            resolved_df = df[df['action'].isin(['Match Approved', 'Match Rejected'])]
            return set(resolved_df['cluster_id'].astype(str).unique())
        except Exception:
            return set()
