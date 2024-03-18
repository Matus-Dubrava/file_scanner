import subprocess
from pathlib import Path


def initalize_git_repository(where: Path) -> bool:
    cmd = ["git", "init", str(where)]
    proc = subprocess.run(cmd, capture_output=True)
    return proc.returncode == 0
