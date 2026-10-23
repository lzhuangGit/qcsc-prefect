from __future__ import annotations

from pathlib import Path

import pytest
from qcsc_prefect_core.data_ref import (
    DataIntegrityError,
    DataRef,
    file_ref,
    file_ref_path,
    get_resolver,
    register_resolver,
)


def test_file_ref_records_size_and_checksum(tmp_path: Path):
    path = tmp_path / "a b.bin"
    path.write_bytes(b"hello")

    ref = file_ref(path)

    assert ref.scheme == "file"
    assert ref.size == 5
    assert ref.sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert file_ref_path(ref) == path
    assert file_ref(path, checksum=False).sha256 is None


def test_file_ref_validation(tmp_path: Path):
    with pytest.raises(ValueError, match="absolute"):
        file_ref("relative.bin")
    with pytest.raises(FileNotFoundError):
        file_ref(tmp_path / "missing.bin")


def test_dict_round_trip():
    ref = DataRef(uri="file:///x", size=1, sha256="ab", metadata={"k": "v"})
    assert DataRef.from_dict(ref.to_dict()) == ref
    assert DataRef.from_dict({"uri": "file:///x"}) == DataRef(uri="file:///x")


def test_file_ref_path_rejects_other_schemes():
    with pytest.raises(ValueError, match="Not a local file"):
        file_ref_path(DataRef(uri="s3://bucket/key"))
    with pytest.raises(ValueError, match="Not a local file"):
        file_ref_path(DataRef(uri="file://otherhost/x"))


def test_resolver_exists_open_materialize_verify(tmp_path: Path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"data")
    ref = file_ref(src)
    resolver = get_resolver(ref)

    assert resolver.exists(ref)
    with resolver.open(ref) as f:
        assert f.read() == b"data"
    copied = resolver.materialize(ref, tmp_path / "sub" / "copy.bin")
    assert copied.read_bytes() == b"data"
    resolver.verify(ref)


def test_verify_detects_missing_and_modified_data(tmp_path: Path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"data")
    ref = file_ref(src)
    resolver = get_resolver(ref)

    src.write_bytes(b"DATA")  # same size, different content
    with pytest.raises(DataIntegrityError, match="sha256"):
        resolver.verify(ref)

    src.write_bytes(b"longer")
    with pytest.raises(DataIntegrityError, match="size"):
        resolver.verify(ref)

    src.unlink()
    assert not resolver.exists(ref)
    with pytest.raises(FileNotFoundError):
        resolver.verify(ref)


def test_unknown_scheme_and_custom_resolver():
    ref = DataRef(uri="memtest://thing")
    with pytest.raises(ValueError, match="No resolver"):
        get_resolver(ref)

    sentinel = object()
    register_resolver("memtest", sentinel)  # type: ignore[arg-type]
    assert get_resolver(ref) is sentinel
