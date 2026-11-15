import shutil
from fnmatch import fnmatchcase
from pathlib import Path

from src.file_helper.base import BaseFileHelper


class LocalFileHelper(BaseFileHelper):
    def scan(self, directory: str, pattern: str) -> list[str]:
        root = Path(directory)
        matches = [
            str(path)
            for path in root.iterdir()
            if path.is_file() and fnmatchcase(path.name, pattern)
        ]
        return sorted(matches)

    def join(self, directory: str, filename: str) -> str:
        return str(Path(directory) / filename)

    def ensure_directory(self, directory: str) -> None:
        Path(directory).mkdir(parents=True, exist_ok=True)

    def archive(self, source: str, archive_directory: str) -> None:
        self.ensure_directory(archive_directory)
        shutil.copy2(source, Path(archive_directory) / Path(source).name)

    def delete(self, path: str) -> None:
        Path(path).unlink(missing_ok=True)
