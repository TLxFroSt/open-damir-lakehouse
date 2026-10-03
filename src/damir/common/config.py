"""Chargement de la configuration : config.yaml, surchargé par les variables d'environnement."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONFIG_ENV_VAR = "DAMIR_CONFIG"
ENV_PREFIX = "DAMIR"


@dataclass(frozen=True)
class PathsConfig:
    """Emplacements des données, un dossier par couche."""

    raw_dir: Path
    bronze_dir: Path


@dataclass(frozen=True)
class SourceConfig:
    """Format des fichiers mensuels Open DAMIR."""

    file_name_template: str
    csv_separator: str
    csv_encoding: str

    def file_name(self, year: int, month: int) -> str:
        """Nom du fichier d'un mois, par exemple A202501.csv.gz pour janvier 2025."""
        return self.file_name_template.format(year=year, month=month)


@dataclass(frozen=True)
class SparkConfig:
    """Paramètres de la session Spark locale."""

    master: str
    app_name: str


@dataclass(frozen=True)
class Settings:
    """Configuration complète du projet."""

    paths: PathsConfig
    source: SourceConfig
    spark: SparkConfig


def load_settings(
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Settings:
    """Charge la configuration.

    Fichier lu : `config_path`, sinon la variable DAMIR_CONFIG, sinon ./config.yaml.
    Une variable DAMIR_<SECTION>_<CLE> remplace la valeur du fichier.
    Les chemins relatifs sont résolus depuis le dossier du fichier de config.
    """
    env = os.environ if env is None else env
    path = Path(config_path or env.get(CONFIG_ENV_VAR, "config.yaml")).resolve()
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    paths = _with_env_overrides("paths", raw.get("paths", {}), env)
    source = _with_env_overrides("source", raw.get("source", {}), env)
    spark = _with_env_overrides("spark", raw.get("spark", {}), env)

    return Settings(
        paths=PathsConfig(**{key: _resolve(path.parent, value) for key, value in paths.items()}),
        source=SourceConfig(**source),
        spark=SparkConfig(**spark),
    )


def _with_env_overrides(
    section: str, values: dict[str, Any], env: Mapping[str, str]
) -> dict[str, Any]:
    """Remplace chaque clé de la section par DAMIR_<SECTION>_<CLE> si cette variable existe."""
    return {
        key: env.get(f"{ENV_PREFIX}_{section}_{key}".upper(), value)
        for key, value in values.items()
    }


def _resolve(base_dir: Path, value: str) -> Path:
    """Rend un chemin absolu ; un chemin relatif part de `base_dir`."""
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path).resolve()
