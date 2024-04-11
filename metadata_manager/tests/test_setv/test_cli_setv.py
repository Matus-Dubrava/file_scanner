import pytest
import subprocess

from models.local_models import RepositoryMetadataORM, FileMetadataORM


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


@pytest.mark.e317680cbf
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("value", ["-v", "--value"])
@pytest.mark.parametrize("file", ["-f", "--file"])
def test_setv_sets_file_key(mdm, working_dir, session, setv_cmd, key, value, file):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)

    subprocess.check_output([*setv_cmd, key, "key_a", value, "value_a", file, file_1])
    subprocess.check_output([*setv_cmd, key, "key_b", value, "value_b", file, file_1])
    subprocess.check_output([*setv_cmd, key, "key_c", value, "value_c", file, file_2])

    # file_1
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
        .value
        == "value_a"
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
        .value
        == "value_b"
    )
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_c")
        .first()
    )

    # file_2
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_a")
        .first()
    )
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_b")
        .first()
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_c")
        .first()
        .value
        == "value_c"
    )


@pytest.mark.d7036b1777
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("value", ["-v", "--value"])
@pytest.mark.parametrize("file", ["-f", "--file"])
def test_setv_updates_existing_file_key(
    mdm, working_dir, session, setv_cmd, key, value, file
):
    file_1 = working_dir.joinpath("file_1")
    mdm.touch(session=session, filepath=file_1)

    subprocess.check_output([*setv_cmd, key, "key_a", value, "value_a", file, file_1])
    subprocess.check_output([*setv_cmd, key, "key_b", value, "value_b", file, file_1])

    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
        .value
        == "value_a"
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
        .value
        == "value_b"
    )

    subprocess.check_output([*setv_cmd, key, "key_b", value, "value_c", file, file_1])

    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
        .value
        == "value_a"
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
        .value
        == "value_c"
    )


@pytest.mark.a8e22087e7
@pytest.mark.setv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("delete", ["-d", "--delete"])
def test_setv_removes_file_key(working_dir, mdm, session, setv_cmd, delete):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)

    mdm.set_value(session=session, key="key_a", value="value_a", filepath=file_1)
    mdm.set_value(session=session, key="key_b", value="value_b", filepath=file_1)
    mdm.set_value(session=session, key="key_a", value="value_a", filepath=file_2)

    subprocess.check_output([*setv_cmd, "--key", "key_a", delete, "--file", file_1])
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
        .value
        == "value_b"
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_a")
        .first()
        .value
        == "value_a"
    )

    subprocess.check_output([*setv_cmd, "--key", "key_b", delete, "--file", file_1])
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
    )
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
    )
    assert (
        session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_a")
        .first()
        .value
        == "value_a"
    )

    subprocess.check_output([*setv_cmd, "--key", "key_a", delete, "--file", file_2])
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_a")
        .first()
    )
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_1, key="key_b")
        .first()
    )
    assert (
        not session.query(FileMetadataORM)
        .filter_by(filepath=file_2, key="key_a")
        .first()
    )
