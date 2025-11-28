from __future__ import annotations

from pathlib import Path

from data_pipeline.config import PIPELINE_SUBDIR_NAME, resolve_data_root


def test_resolve_data_root_appends_pipeline(tmp_path: Path) -> None:
    resolved = resolve_data_root(tmp_path)
    assert resolved == tmp_path / PIPELINE_SUBDIR_NAME


def test_resolve_data_root_legacy_layout(tmp_path: Path) -> None:
    legacy_root = tmp_path / "legacy_root"
    (legacy_root / "data_processed").mkdir(parents=True)

    resolved = resolve_data_root(legacy_root)
    assert resolved == legacy_root


def test_resolve_data_root_accepts_pipeline_root(tmp_path: Path) -> None:
    pipeline_root = tmp_path / PIPELINE_SUBDIR_NAME

    resolved = resolve_data_root(pipeline_root)
    assert resolved == pipeline_root
