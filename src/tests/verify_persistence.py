
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.backend.auth.user_manager import UserManager
from src.backend.engine.resolution_manager import ResolutionManager

def test_user_manager():
    print("Testing UserManager...")
    um = UserManager(data_path="data/test_users.json")
    
    # 1. Add User
    um.add_user("test_user_1", {"name": "Test User", "role": "Dev"})
    
    # 2. Verify file exists
    if not os.path.exists(um.data_path):
        print("FAIL: Users file not created")
        return False
        
    # 3. Reload
    um2 = UserManager(data_path="data/test_users.json")
    users = um2.get_users()
    
    if "test_user_1" in users and users["test_user_1"]["name"] == "Test User":
        print("PASS: User persistence verified")
    else:
        print("FAIL: User persistence failed")
        return False
        
    # Cleanup
    os.remove(um.data_path)
    return True

def test_resolution_manager():
    print("Testing ResolutionManager...")
    rm = ResolutionManager(output_path="data/output/test_resolutions.csv")
    
    # 1. Log Resolution
    rm.log_resolution("CLUSTER_999", "Match Approved", "tester")
    
    # 2. Verify file exists
    if not os.path.exists(rm.output_path):
        print("FAIL: Resolutions file not created")
        return False
        
    # 3. Get Resolved IDs
    ids = rm.get_resolved_ids()
    
    if "CLUSTER_999" in ids:
        print("PASS: Resolution persistence verified")
    else:
        print(f"FAIL: Resolution persistence failed. IDs found: {ids}")
        return False
        
    # Cleanup
    os.remove(rm.output_path)
    return True

if __name__ == "__main__":
    u_ok = test_user_manager()
    r_ok = test_resolution_manager()
    
    if u_ok and r_ok:
        print("\nALL SYSTEM CHECKS PASSED")
    else:
        print("\nSYSTEM CHECKS FAILED")
