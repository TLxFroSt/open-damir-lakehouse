"""Chargement de la configuration : config.yaml, surchargé par les variables d'environnement."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from damir.common.tables import TableRef

CONFIG_ENV_VAR = "DAMIR_CONFIG"
ENV_PREFIX = "DAMIR"


@dataclass(frozen=True)
class PathsConfig:
    """Emplacements des données, un dossier par couche."""

    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    nomenclatures_file: Path
    gold_db: Path


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
    driver_memory: str


@dataclass(frozen=True)
class BronzeConfig:
    """Paramètres d'écriture de la couche bronze."""

    table_name: str


@dataclass(frozen=True)
class SilverConfig:
    """Noms des tables de la couche silver."""

    table_name: str
    quarantine_table_name: str
    nomenclatures_table_name: str


@dataclass(frozen=True)
class StorageConfig:
    """Où vivent les tables Delta.

    - path : dossiers Delta sous paths.bronze_dir / paths.silver_dir (exécution locale) ;
    - catalog : tables Unity Catalog <catalog>.bronze.<table> et <catalog>.silver.<table>
      (Databricks), avec la session Spark fournie par la plateforme.
    """

    mode: str
    catalog: str

    def __post_init__(self) -> None:
        if self.mode not in {"path", "catalog"}:
            raise ValueError(f"storage.mode invalide : {self.mode!r} (attendu : path ou catalog)")

    @property
    def uses_catalog(self) -> bool:
        return self.mode == "catalog"


@dataclass(frozen=True)
class Settings:
    """Configuration complète du projet."""

    paths: PathsConfig
    source: SourceConfig
    spark: SparkConfig
    bronze: BronzeConfig
    silver: SilverConfig
    storage: StorageConfig

    def _table(self, layer: str, directory: Path, name: str) -> TableRef:
        if self.storage.uses_catalog:
            return TableRef(f"{self.storage.catalog}.{layer}.{name}", is_path=False)
        return TableRef((directory / name).as_posix(), is_path=True)

    @property
    def bronze_table(self) -> TableRef:
        """Table Delta bronze."""
        return self._table("bronze", self.paths.bronze_dir, self.bronze.table_name)

    @property
    def silver_table(self) -> TableRef:
        """Table Delta silver."""
        return self._table("silver", self.paths.silver_dir, self.silver.table_name)

    @property
    def quarantine_table(self) -> TableRef:
        """Table Delta de quarantaine (lignes rejetées du silver)."""
        return self._table("silver", self.paths.silver_dir, self.silver.quarantine_table_name)

    @property
    def nomenclatures_table(self) -> TableRef:
        """Table Delta des nomenclatures (libellés des codes)."""
        return self._table("silver", self.paths.silver_dir, self.silver.nomenclatures_table_name)


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
    bronze = _with_env_overrides("bronze", raw.get("bronze", {}), env)
    silver = _with_env_overrides("silver", raw.get("silver", {}), env)
    storage = _with_env_overrides("storage", raw.get("storage", {}), env)

    return Settings(
        paths=PathsConfig(**{key: _resolve(path.parent, value) for key, value in paths.items()}),
        source=SourceConfig(**source),
        spark=SparkConfig(**spark),
        bronze=BronzeConfig(**bronze),
        silver=SilverConfig(**silver),
        storage=StorageConfig(**storage),
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
