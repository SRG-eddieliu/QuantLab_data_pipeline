from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


DATA_ROOT_ENV_VAR = "QUANTLAB_DATA_ROOT"
DEFAULT_DATA_ROOT_NAME = "quantlab_data"
PIPELINE_SUBDIR_NAME = "quantlab_data_pipeline"
LEGACY_DATA_DIRS = ("data_processed", "data_meta", "data_raw", "reference")


def _default_base_root() -> Path:
    """
    Resolve the shared base data directory outside the repo.

    Preference order:
    1) env QUANTLAB_DATA_ROOT
    2) sibling of the repo named "quantlab_data" (e.g., ../quantlab_data)
    3) cwd/quantlab_data
    """

    env_root = os.getenv(DATA_ROOT_ENV_VAR)
    if env_root:
        return Path(env_root).expanduser().resolve()

    # Look for a repo root (pyproject.toml) above this file
    repo_root = None
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            repo_root = parent
            break

    if repo_root:
        return (repo_root.parent / DEFAULT_DATA_ROOT_NAME).resolve()

    return (Path.cwd() / DEFAULT_DATA_ROOT_NAME).resolve()


def _pipeline_root(base_root: Path) -> Path:
    """
    Append the pipeline-specific subdirectory unless the base already looks
    like a data root (legacy layout) or is already the subdir itself.
    """
    if base_root.name == PIPELINE_SUBDIR_NAME:
        return base_root
    if any((base_root / marker).exists() for marker in LEGACY_DATA_DIRS):
        return base_root
    return (base_root / PIPELINE_SUBDIR_NAME).resolve()


def default_data_root() -> Path:
    """
    Resolve the pipeline-specific data root under the shared quant data folder.

    Preference order for the base directory:
    1) env QUANTLAB_DATA_ROOT
    2) sibling of the repo named "quantlab_data" (e.g., ../quantlab_data)
    3) cwd/quantlab_data

    The pipeline writes inside <base>/quantlab_data_pipeline/ by default so the
    shared folder can host other datasets. If the base already contains
    data_processed/data_meta/etc. (legacy layout) or already points at the
    subfolder, it is returned unchanged for compatibility.
    """
    base_root = _default_base_root()
    return _pipeline_root(base_root)


def resolve_data_root(root: Path | str | None = None) -> Path:
    """
    Normalize a user-supplied base/root path to the pipeline's output folder.
    """
    if root is None:
        return default_data_root()
    return _pipeline_root(Path(root).expanduser().resolve())


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    Load a YAML configuration file into a dictionary.

    This keeps configuration in code rather than scattered across modules.
    """
    config_path = Path(path).expanduser().resolve()
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
