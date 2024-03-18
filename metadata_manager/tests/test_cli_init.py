import subprocess
import pytest
from pathlib import Path


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(working_dir, init_cmd, monkeypatch):
    monkeypatch.chdir(working_dir)
    proc = subprocess.run([*init_cmd], capture_output=True)
    assert not proc.stderr
    assert proc.returncode == 0
    assert (working_dir / ".md").exists()
