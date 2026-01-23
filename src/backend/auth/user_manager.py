import json
import os
import streamlit as st

class UserManager:
    def __init__(self, data_path="data/users.json"):
        # Ensure data directory exists
        self.data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../", data_path))
        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        self.users = self._load_users()

    def _load_users(self):
        """Load users from JSON file or return defaults if file doesn't exist."""
        if os.path.exists(self.data_path):
            try:
                with open(self.data_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading users: {e}")
                return self._get_default_users()
        else:
            defaults = self._get_default_users()
            self._save_to_disk(defaults)
            return defaults

    def _get_default_users(self):
        """Return the default hardcoded users."""
        return {
            "admin": {"pass": "admin123", "role": "Admin", "name": "Administrator", "email": "admin@icore.io", "status": "Active", "last_login": "2 mins ago"},
            "exec": {"pass": "exec123", "role": "Executive", "name": "C-Suite User", "email": "exec@icore.io", "status": "Active", "last_login": "1 hour ago"},
            "steward": {"pass": "steward123", "role": "Steward", "name": "Data Steward", "email": "steward@icore.io", "status": "Active", "last_login": "Just now"},
            "dev": {"pass": "dev123", "role": "Developer", "name": "Tech Lead", "email": "dev@icore.io", "status": "Active", "last_login": "15 mins ago"}
        }

    def _save_to_disk(self, users_data=None):
        """Save the current users state to disk."""
        if users_data is None:
            users_data = self.users
        try:
            with open(self.data_path, 'w') as f:
                json.dump(users_data, f, indent=4)
        except Exception as e:
            print(f"Error saving users: {e}")

    def get_users(self):
        """Return the current users dictionary."""
        # Always reload to get fresh state if multiple sessions interact (simple concurrency)
        self.users = self._load_users()
        return self.users

    def add_user(self, username, user_data):
        """Add or update a user."""
        self.users = self._load_users()
        self.users[username] = user_data
        self._save_to_disk()
        return True

    def delete_user(self, username):
        """Delete a user."""
        self.users = self._load_users()
        if username in self.users:
            del self.users[username]
            self._save_to_disk()
            return True
        return False
        
    def sync_session(self):
        """Sync session state with disk data."""
        st.session_state['PLATFORM_USERS'] = self.get_users()
