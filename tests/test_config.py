"""Tests du chargement de la configuration."""

from pathlib import Path
from typing import Any

import pytest
import yaml

from damir.common.config import load_settings

REPO_CONFIG = Path(__file__).parents[1] / "config.yaml"


def write_config(directory: Path, **overrides: dict[str, Any]) -> Path:
    """Écrit un config.yaml minimal valide, sections remplaçables par `overrides`."""
    content = {
        "paths": {"raw_dir": "raw", "bronze_dir": "bronze"},
        "source": {
            "file_name_template": "A{year}{month:02d}.csv.gz",
            "csv_separator": ";",
            "csv_encoding": "UTF-8",
        },
        "spark": {"master": "local[1]", "app_name": "test", "driver_memory": "1g"},
        "bronze": {"table_name": "open_damir", "files_per_month": 2},
    }
    content.update(overrides)
    path = directory / "config.yaml"
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return path


def test_repo_config_loads() -> None:
    """Le config.yaml du dépôt est valide et pointe vers data/raw."""
    settings = load_settings(REPO_CONFIG, env={})

    assert settings.paths.raw_dir == REPO_CONFIG.parent / "data" / "raw"
    assert settings.source.csv_separator == ";"


@pytest.mark.parametrize(
    ("year", "month", "expected"),
    [(2025, 1, "A202501.csv.gz"), (2025, 12, "A202512.csv.gz")],
)
def test_file_name(year: int, month: int, expected: str) -> None:
    """Le nom de fichier suit le format A + année + mois sur 2 chiffres."""
    settings = load_settings(REPO_CONFIG, env={})

    assert settings.source.file_name(year, month) == expected


def test_relative_paths_resolved_from_config_dir(tmp_path: Path) -> None:
    """Un chemin relatif part du dossier du fichier, pas du répertoire courant."""
    settings = load_settings(write_config(tmp_path), env={})

    assert settings.paths.raw_dir == tmp_path / "raw"
    assert settings.paths.bronze_dir == tmp_path / "bronze"


def test_env_variable_overrides_file(tmp_path: Path) -> None:
    """DAMIR_<SECTION>_<CLE> remplace la valeur du fichier."""
    other_dir = tmp_path / "ailleurs"
    env = {"DAMIR_PATHS_RAW_DIR": str(other_dir), "DAMIR_SPARK_MASTER": "local[4]"}

    settings = load_settings(write_config(tmp_path), env=env)

    assert settings.paths.raw_dir == other_dir
    assert settings.spark.master == "local[4]"


def test_env_override_of_integer_is_converted(tmp_path: Path) -> None:
    """Une variable d'environnement (texte) devient un entier là où c'est attendu."""
    env = {"DAMIR_BRONZE_FILES_PER_MONTH": "8"}

    settings = load_settings(write_config(tmp_path), env=env)

    assert settings.bronze.files_per_month == 8


def test_config_file_from_env_variable(tmp_path: Path) -> None:
    """Sans chemin explicite, DAMIR_CONFIG désigne le fichier à lire."""
    config_path = write_config(tmp_path)

    settings = load_settings(env={"DAMIR_CONFIG": str(config_path)})

    assert settings.spark.app_name == "test"


def test_missing_key_is_rejected(tmp_path: Path) -> None:
    """Une clé obligatoire absente fait échouer le chargement."""
    config_path = write_config(tmp_path, paths={"raw_dir": "raw"})

    with pytest.raises(TypeError, match="bronze_dir"):
        load_settings(config_path, env={})
