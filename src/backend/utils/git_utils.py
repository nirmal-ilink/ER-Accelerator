import subprocess
import os

def get_current_branch():
    """
    Returns the name of the current git branch.
    Returns None if git is not available or not a git repository.
    """
    try:
        # Run git command to get current branch
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], 
            stderr=subprocess.DEVNULL
        ).decode().strip()
        print(f"DEBUG: Current git branch detected: {branch}")
        return branch
    except Exception as e:
        print(f"WARNING: Failed to detect git branch: {e}")
        return None
