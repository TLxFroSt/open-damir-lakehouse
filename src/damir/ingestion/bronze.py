"""Couche bronze : un fichier mensuel Open DAMIR devient une partition Delta, sans retouche."""

import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from damir.common.config import Settings, SourceConfig
from damir.common.delta import overwrite_month, rows_written_by_last_commit

logger = logging.getLogger(__name__)

# Nom donné par Spark à une colonne dont l'en-tête est vide : _c0, _c1, ...
_UNNAMED_COLUMN = re.compile(r"_c\d+")


def read_raw_csv(spark: SparkSession, path: Path, source: SourceConfig) -> DataFrame:
    """Lit un fichier CSV DAMIR (gzip accepté) en gardant toutes les colonnes en texte.

    Pas d'inférence de types : le bronze reste fidèle à la source (zéros en tête
    des codes conservés, par exemple). Le typage se fait en silver.
    """
    return spark.read.csv(
        path.as_posix(),
        header=True,
        sep=source.csv_separator,
        encoding=source.csv_encoding,
        inferSchema=False,
    )


def drop_unnamed_columns(df: DataFrame) -> DataFrame:
    """Supprime les colonnes sans nom d'en-tête.

    Chaque ligne DAMIR se termine par « ; » : Spark y voit une colonne
    supplémentaire, vide et sans nom.
    """
    return df.drop(*[name for name in df.columns if _UNNAMED_COLUMN.fullmatch(name)])


def add_technical_columns(
    df: DataFrame, source_file: str, year_month: str, ingested_at: datetime
) -> DataFrame:
    """Ajoute les colonnes techniques : fichier d'origine, date de chargement, mois de flux."""
    return df.withColumns(
        {
            "_source_file": F.lit(source_file),
            "_ingested_at": F.lit(ingested_at),
            "_year_month": F.lit(year_month),
        }
    )


def raw_file_path(settings: Settings, year: int, month: int) -> Path:
    """Chemin attendu du fichier source d'un mois (sans vérifier qu'il existe)."""
    if not 1 <= month <= 12:
        raise ValueError(f"Mois invalide : {month} (attendu entre 1 et 12)")
    return settings.paths.raw_dir / settings.source.file_name(year, month)


def check_raw_files(settings: Settings, year: int, months: list[int]) -> None:
    """Vérifie que le fichier source de chaque mois existe, et les signale tous s'il en manque."""
    missing = [
        path for path in (raw_file_path(settings, year, m) for m in months) if not path.is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Fichier(s) source introuvable(s) : " + ", ".join(str(p) for p in missing)
        )


def ingest_months(
    spark: SparkSession, settings: Settings, year: int, months: list[int]
) -> dict[int, int]:
    """Charge plusieurs mois d'une année et renvoie le nombre de lignes écrites par mois.

    Tous les fichiers sont vérifiés avant le premier chargement : un fichier
    manquant est signalé tout de suite, pas après plusieurs minutes de traitement.
    """
    check_raw_files(settings, year, months)
    return {month: ingest_month(spark, settings, year, month) for month in months}


def ingest_month(
    spark: SparkSession,
    settings: Settings,
    year: int,
    month: int,
    ingested_at: datetime | None = None,
) -> int:
    """Charge le fichier d'un mois dans la table bronze et renvoie le nombre de lignes écrites."""
    raw_path = raw_file_path(settings, year, month)
    if not raw_path.is_file():
        raise FileNotFoundError(f"Fichier source introuvable : {raw_path}")

    file_name = raw_path.name
    year_month = f"{year}{month:02d}"
    table = settings.bronze_table
    logger.info("Chargement de %s dans %s (partition %s)", file_name, table.location, year_month)
    started = time.perf_counter()

    df = read_raw_csv(spark, raw_path, settings.source)
    df = drop_unnamed_columns(df)
    df = add_technical_columns(df, file_name, year_month, ingested_at or datetime.now(UTC))
    # Un .gz n'est pas découpable : Spark le lit en une seule partition (un seul cœur).
    # On répartit les lignes pour que l'écriture Parquet se fasse en parallèle.
    overwrite_month(df.repartition(settings.bronze.files_per_month), table, year_month)

    rows = rows_written_by_last_commit(spark, table)
    logger.info("%s lignes écrites en %.0f s", f"{rows:,}", time.perf_counter() - started)
    return rows
