import subprocess
import pytest
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(init_cmd, monkeypatch):
    monkeypatch.chdir("/tmp")
    proc = subprocess.run([*init_cmd], capture_output=True)
    print(proc)
    assert not proc.stderr
    assert proc.returncode == 0
    assert (Path("/tmp") / ".md").exists()
