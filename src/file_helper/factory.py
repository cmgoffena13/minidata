from src.file_helper.base import BaseFileHelper
from src.file_helper.local import LocalFileHelper
from src.file_helper.object_store import ObjectStoreFileHelper


def create_file_helper(
    directory: str,
    storage_options: dict[str, str] | None = None,
) -> BaseFileHelper:
    if "://" in directory:
        return ObjectStoreFileHelper(storage_options)
    return LocalFileHelper()
