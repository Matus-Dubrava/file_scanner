import pytest
import subprocess

from md_models import RepositoryMetadataORM


@pytest.mark.d1c6d01caa
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("value", ["-v", "--value"])
def test_setv_sets_repository_key(session, setv_cmd, key, value):
    subprocess.check_output([*setv_cmd, key, "testkey", value, "testvalue"])
    subprocess.check_output([*setv_cmd, key, "testkey2", value, "testvalue2"])

    record = session.query(RepositoryMetadataORM).filter_by(key="testkey").first()
    assert record.value == "testvalue"

    record = session.query(RepositoryMetadataORM).filter_by(key="testkey2").first()
    assert record.value == "testvalue2"


@pytest.mark.ddda873634
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("value", ["-v", "--value"])
def test_setv_updates_existing_repository_key(session, setv_cmd, key, value):
    subprocess.check_output([*setv_cmd, key, "testkey", value, "testvalue"])
    record = session.query(RepositoryMetadataORM).filter_by(key="testkey").first()
    assert record.value == "testvalue"

    subprocess.check_output([*setv_cmd, key, "testkey", value, "testvalue2"])
    session.expunge_all()

    record = session.query(RepositoryMetadataORM).filter_by(key="testkey").first()
    assert record.value == "testvalue2"


@pytest.mark.ee13db7868
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("delete", ["-d", "--delete"])
def test_setv_removes_repository_key(mdm, session, setv_cmd, key, delete):
    mdm.set_value(session=session, key="testkey", value="testvalue")
    mdm.set_value(session=session, key="testkey2", value="testvalue2")

    subprocess.check_output([*setv_cmd, key, "testkey", delete])
    session.expunge_all()

    assert not session.query(RepositoryMetadataORM).filter_by(key="testkey").first()
    assert (
        session.query(RepositoryMetadataORM).filter_by(key="testkey2").first().value
        == "testvalue2"
    )


@pytest.mark.dc7f46c9ae
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("delete", ["-d", "--delete"])
def test_setv_removes_nothing_when_repository_key_doesnt_exists(
    session, setv_cmd, key, delete
):
    subprocess.check_output([*setv_cmd, key, "testkey", delete])
    assert not session.query(RepositoryMetadataORM).filter_by(key="testkey").first()
