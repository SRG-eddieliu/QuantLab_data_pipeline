from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


DATA_ROOT_ENV_VAR = "QUANTLAB_DATA_ROOT"
DEFAULT_DATA_ROOT_NAME = "quantlab_data"


def default_data_root() -> Path:
    """
    Resolve the shared data root outside the repo.

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


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    Load a YAML configuration file into a dictionary.

    This keeps configuration in code rather than scattered across modules.
    """
    config_path = Path(path).expanduser().resolve()
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
