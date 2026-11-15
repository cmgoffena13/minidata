from abc import ABC, abstractmethod


class BaseFileHelper(ABC):
    @abstractmethod
    def scan(self, directory: str, pattern: str) -> list[str]:
        pass

    @abstractmethod
    def join(self, directory: str, filename: str) -> str:
        pass

    @abstractmethod
    def ensure_directory(self, directory: str) -> None:
        pass

    @abstractmethod
    def archive(self, source: str, archive_directory: str) -> None:
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        pass
