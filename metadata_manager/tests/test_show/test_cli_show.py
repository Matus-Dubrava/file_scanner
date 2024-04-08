import pytest
import subprocess

from md_models import FileORM, RepositoryORM

# NOTE: Some of these tests are very simplistic as the textual representation of
# repository objects is subject to change.


@pytest.mark.bb88cf11a9
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.sanity
def test_show_displays_repository_record(session, show_cmd):
    output = subprocess.check_output([*show_cmd])
    repository_record = session.query(RepositoryORM).first()

    assert repository_record.id in output.decode().lower()


@pytest.mark.d3b2afb668
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.sanity
def test_show_displays_repository_stats(show_cmd, mdm):
    output = subprocess.check_output([*show_cmd])
    assert "active files" in output.decode().lower()
    assert "removed files" in output.decode().lower()
    assert "total lines" in output.decode().lower()
    assert "added lines" in output.decode().lower()
    assert "removed lines" in output.decode().lower()


@pytest.mark.ba97fa7847
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.sanity
def test_show_displays_file_record(working_dir, mdm, session, show_cmd):
    file_ = working_dir.joinpath("file_")
    mdm.touch(session=session, filepath=file_)

    output = subprocess.check_output([*show_cmd, file_])
    file_record = session.query(FileORM).filter_by(filepath=file_).first()

    assert str(file_record.filepath) in output.decode().lower()


@pytest.mark.e98fea3c4e
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.sanity
def test_show_fails_when_file_record_doesnt_exist(show_cmd):
    proc = subprocess.run([*show_cmd, "testfile"], capture_output=True)
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()
