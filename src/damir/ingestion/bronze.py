"""Couche bronze : un fichier mensuel Open DAMIR devient une partition Delta, sans retouche."""

import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from damir.common.config import Settings, SourceConfig

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


def write_bronze(df: DataFrame, table_path: str, year_month: str) -> None:
    """Écrit un mois dans la table Delta en remplaçant uniquement sa partition.

    Idempotence : relancer un mois remplace ses lignes au lieu de les ajouter.
    Un MERGE ne conviendrait pas : les lignes DAMIR sont des agrégats sans clé
    unique, deux lignes identiques peuvent être légitimes.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_year_month = '{year_month}'")
        .partitionBy("_year_month")
        .save(table_path)
    )


def ingest_month(
    spark: SparkSession,
    settings: Settings,
    year: int,
    month: int,
    ingested_at: datetime | None = None,
) -> int:
    """Charge le fichier d'un mois dans la table bronze et renvoie le nombre de lignes écrites."""
    if not 1 <= month <= 12:
        raise ValueError(f"Mois invalide : {month} (attendu entre 1 et 12)")

    file_name = settings.source.file_name(year, month)
    raw_path = settings.paths.raw_dir / file_name
    if not raw_path.is_file():
        raise FileNotFoundError(f"Fichier source introuvable : {raw_path}")

    year_month = f"{year}{month:02d}"
    table_path = (settings.paths.bronze_dir / settings.bronze.table_name).as_posix()
    logger.info("Chargement de %s dans %s (partition %s)", file_name, table_path, year_month)
    started = time.perf_counter()

    df = read_raw_csv(spark, raw_path, settings.source)
    df = drop_unnamed_columns(df)
    df = add_technical_columns(df, file_name, year_month, ingested_at or datetime.now(UTC))
    # Un .gz n'est pas découpable : Spark le lit en une seule partition (un seul cœur).
    # On répartit les lignes pour que l'écriture Parquet se fasse en parallèle.
    write_bronze(df.repartition(settings.bronze.files_per_month), table_path, year_month)

    rows = _rows_written_by_last_commit(spark, table_path)
    logger.info("%s lignes écrites en %.0f s", f"{rows:,}", time.perf_counter() - started)
    return rows


def _rows_written_by_last_commit(spark: SparkSession, table_path: str) -> int:
    """Nombre de lignes écrites par le dernier commit, lu dans l'historique Delta.

    Évite un count() qui relirait toute la partition.
    """
    last_commit = DeltaTable.forPath(spark, table_path).history(1).first()
    return int(last_commit["operationMetrics"]["numOutputRows"])
