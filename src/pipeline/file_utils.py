import re
from collections import Counter
from pathlib import Path

from xxhash import xxh128_hexdigest

from src.file_helper.factory import create_file_helper


def find_matching_files(
    directory: str,
    pattern: str,
    storage_options: dict[str, str] | None = None,
) -> list[str]:
    return create_file_helper(directory, storage_options).scan(directory, pattern)


def file_name(path: str) -> str:
    return path.rstrip("/").split("/")[-1]


def sanitize_staging_name(filename: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", Path(filename).stem)
    return re.sub(r"_+", "_", cleaned).strip("_")


def staging_names_for_files(paths: list[str]) -> dict[str, str]:
    sanitized = {path: sanitize_staging_name(file_name(path)) for path in paths}
    counts = Counter(sanitized.values())
    names: dict[str, str] = {}
    for path, name in sanitized.items():
        if counts[name] > 1:
            digest = xxh128_hexdigest(file_name(path).encode())[:8]
            names[path] = f"{name}_{digest}"
        else:
            names[path] = name
    return names
