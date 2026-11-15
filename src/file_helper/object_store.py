from fnmatch import fnmatchcase
from pathlib import Path
from urllib.parse import urlparse

from deltalake.fs import DeltaStorageHandler
from pyarrow.fs import FileSelector, FileType

from src.file_helper.base import BaseFileHelper


class ObjectStoreFileHelper(BaseFileHelper):
    def __init__(self, storage_options: dict[str, str] | None = None) -> None:
        self.storage_options = storage_options

    def scan(self, directory: str, pattern: str) -> list[str]:
        root = directory.rstrip("/")
        handler = DeltaStorageHandler(root, self.storage_options)
        infos = handler.get_file_info_selector(FileSelector("", recursive=False))
        matches = [
            self.join(root, info.path.split("/")[-1])
            for info in infos
            if info.type == FileType.File and fnmatchcase(info.path.split("/")[-1], pattern)
        ]
        return sorted(matches)

    def join(self, directory: str, filename: str) -> str:
        return f"{directory.rstrip('/')}/{filename}"

    def ensure_directory(self, directory: str) -> None:
        parsed = urlparse(directory)
        if parsed.scheme == "file":
            Path(parsed.path).mkdir(parents=True, exist_ok=True)
            return
        DeltaStorageHandler(directory.rstrip("/"), self.storage_options).create_dir("")

    def archive(self, source: str, archive_directory: str) -> None:
        source_root, source_key = _storage_root_and_key(source)
        archive_root, archive_prefix = _storage_root_and_key(archive_directory.rstrip("/"))
        if source_root != archive_root:
            raise ValueError("Archive directory must share the same storage root as the source")
        filename = source_key.rsplit("/", 1)[-1]
        destination_key = f"{archive_prefix.rstrip('/')}/{filename}" if archive_prefix else filename
        self.ensure_directory(archive_directory)
        handler = DeltaStorageHandler(source_root, self.storage_options)
        handler._handler.copy_file(source_key, destination_key)

    def delete(self, path: str) -> None:
        directory, filename = path.rstrip("/").rsplit("/", 1)
        DeltaStorageHandler(directory, self.storage_options).delete_file(filename)


def _storage_root_and_key(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return "file://", parsed.path
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid object store URI: {uri}")
    return f"{parsed.scheme}://{parsed.netloc}", parsed.path.lstrip("/")
