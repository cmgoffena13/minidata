from pathlib import Path

from src.file_helper.factory import create_file_helper
from src.file_helper.local import LocalFileHelper
from src.file_helper.object_store import ObjectStoreFileHelper
from src.pipeline.file_utils import (
    find_matching_files,
    sanitize_staging_name,
    staging_names_for_files,
)


def test_sanitize_staging_name_strips_extension_and_special_characters() -> None:
    assert sanitize_staging_name("orders (1).csv") == "orders_1"


def test_staging_names_append_hash_on_collision(tmp_path: Path) -> None:
    first = str(tmp_path / "a-b.csv")
    second = str(tmp_path / "a_b.csv")
    Path(first).write_text("a")
    Path(second).write_text("b")
    names = staging_names_for_files([first, second])
    assert names[first] != names[second]
    assert names[first].startswith("a_b_")
    assert names[second].startswith("a_b_")


def test_find_matching_files_is_case_sensitive(tmp_path: Path) -> None:
    (tmp_path / "widget_a.csv").write_text("a")
    (tmp_path / "Widget_b.csv").write_text("b")
    (tmp_path / "other.csv").write_text("c")
    matches = find_matching_files(str(tmp_path), "widget_*")
    assert [Path(path).name for path in matches] == ["widget_a.csv"]


def test_create_file_helper_selects_object_store_for_uris() -> None:
    assert isinstance(create_file_helper("incoming"), LocalFileHelper)
    assert isinstance(create_file_helper("s3://bucket/incoming"), ObjectStoreFileHelper)
    assert isinstance(create_file_helper("file:///var/incoming"), ObjectStoreFileHelper)


def test_object_store_helper_scans_archives_and_deletes(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    archive = tmp_path / "archive"
    incoming.mkdir()
    (incoming / "widget_a.csv").write_text("a")
    (incoming / "other.csv").write_text("b")
    helper = ObjectStoreFileHelper()
    directory = incoming.as_uri()
    archive_directory = archive.as_uri()
    matches = helper.scan(directory, "widget_*")
    assert matches == [f"{directory}/widget_a.csv"]
    helper.archive(matches[0], archive_directory)
    assert (archive / "widget_a.csv").read_text() == "a"
    helper.delete(matches[0])
    assert not (incoming / "widget_a.csv").exists()
    assert (incoming / "other.csv").exists()
