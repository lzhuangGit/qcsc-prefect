from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Protocol
from urllib.parse import unquote, urlparse

_CHUNK = 1024 * 1024


class DataIntegrityError(RuntimeError):
    """Raised when data does not match the size or checksum recorded in a `DataRef`."""


@dataclass(frozen=True)
class DataRef:
    """Serializable reference to data that lives somewhere else.

    Tasks exchange a `DataRef` instead of the data itself, so the storage
    behind it (shared volume today, remote object or job output tomorrow) can
    change without changing flows or executors.

    Attributes:
        uri: Location, for example ``file:///data/job1/input.bin``.
        size: Optional size in bytes, checked by `DataResolver.verify`.
        sha256: Optional hex digest, checked by `DataResolver.verify`.
        metadata: Free-form string metadata.
    """

    uri: str
    size: int | None = None
    sha256: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "size": self.size,
            "sha256": self.sha256,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DataRef:
        return cls(
            uri=data["uri"],
            size=data.get("size"),
            sha256=data.get("sha256"),
            metadata=dict(data.get("metadata") or {}),
        )


class DataResolver(Protocol):
    """Access to the data behind a `DataRef` of one URI scheme."""

    def exists(self, ref: DataRef) -> bool: ...

    def open(self, ref: DataRef) -> IO[bytes]: ...

    def materialize(self, ref: DataRef, dest: Path) -> Path:
        """Make the data available as a local file at ``dest``."""
        ...

    def verify(self, ref: DataRef) -> None:
        """Raise `FileNotFoundError` or `DataIntegrityError` if the data is not as described."""
        ...


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def file_ref(path: str | Path, *, checksum: bool = True) -> DataRef:
    """Create a ``file://`` reference to an existing local file.

    Args:
        path: Absolute path to the file.
        checksum: Also record the SHA-256 digest (reads the whole file).

    Raises:
        ValueError: If ``path`` is not absolute.
        FileNotFoundError: If the file does not exist.
    """

    p = Path(path)
    if not p.is_absolute():
        raise ValueError(f"file_ref requires an absolute path, got {str(path)!r}.")
    if not p.is_file():
        raise FileNotFoundError(str(p))
    return DataRef(
        uri=p.as_uri(),
        size=p.stat().st_size,
        sha256=_sha256_of(p) if checksum else None,
    )


def file_ref_path(ref: DataRef) -> Path:
    """Return the local path of a ``file://`` reference.

    Raises:
        ValueError: If ``ref`` uses another scheme.
    """

    parsed = urlparse(ref.uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise ValueError(f"Not a local file reference: {ref.uri!r}.")
    return Path(unquote(parsed.path))


class FileResolver:
    """Resolver for ``file://`` references on a filesystem visible to this process."""

    def exists(self, ref: DataRef) -> bool:
        return file_ref_path(ref).is_file()

    def open(self, ref: DataRef) -> IO[bytes]:
        return file_ref_path(ref).open("rb")

    def materialize(self, ref: DataRef, dest: Path) -> Path:
        src = file_ref_path(ref)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        return dest

    def verify(self, ref: DataRef) -> None:
        path = file_ref_path(ref)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        if ref.size is not None and path.stat().st_size != ref.size:
            raise DataIntegrityError(
                f"{ref.uri}: size is {path.stat().st_size}, expected {ref.size}."
            )
        if ref.sha256 is not None and _sha256_of(path) != ref.sha256:
            raise DataIntegrityError(f"{ref.uri}: sha256 does not match.")


_RESOLVERS: dict[str, DataResolver] = {"file": FileResolver()}


def register_resolver(scheme: str, resolver: DataResolver) -> None:
    """Register ``resolver`` for URI ``scheme``, replacing any existing one."""

    _RESOLVERS[scheme] = resolver


def get_resolver(ref: DataRef) -> DataResolver:
    """Return the resolver registered for ``ref``'s scheme.

    Raises:
        ValueError: If no resolver handles the scheme.
    """

    try:
        return _RESOLVERS[ref.scheme]
    except KeyError:
        raise ValueError(
            f"No resolver registered for scheme {ref.scheme!r} (uri={ref.uri!r})."
        ) from None
