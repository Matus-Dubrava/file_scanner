import subprocess
from pathlib import Path


def initalize_git_repository(where: Path) -> bool:
    """
    Intialize git in target directory.
    Returns True if the operation was successful.
    """
    cmd = ["git", "init", str(where)]
    proc = subprocess.run(cmd, capture_output=True)
    return proc.returncode == 0
