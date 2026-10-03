"""Briques communes aux lignes de commande (ingestion bronze, silver)."""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from damir.common.config import Settings
from damir.common.spark import build_spark_session


def parse_month_args(description: str, argv: list[str] | None = None) -> argparse.Namespace:
    """Arguments communs : --year, un ou plusieurs --month, --config."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--year", type=int, required=True, help="année, par exemple 2025")
    parser.add_argument(
        "--month",
        type=int,
        nargs="+",
        required=True,
        help="un ou plusieurs mois de 1 à 12, par exemple : --month 1 2 3",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="fichier de configuration (défaut : variable DAMIR_CONFIG, sinon ./config.yaml)",
    )
    return parser.parse_args(argv)


def configure_logging() -> None:
    """INFO pour les messages du projet ; les bibliothèques (py4j...) restent en WARNING."""
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )
    logging.getLogger("damir").setLevel(logging.INFO)


def check_months(months: list[int]) -> None:
    """Refuse un mois hors de 1..12."""
    invalid = [m for m in months if not 1 <= m <= 12]
    if invalid:
        raise ValueError(f"Mois invalide(s) : {invalid} (attendu entre 1 et 12)")


def spark_from_settings(settings: Settings) -> SparkSession:
    """Session Spark selon le mode de stockage.

    - catalog (Databricks) : session fournie par la plateforme (Spark Connect en serverless) ;
      jars, mémoire et journalisation y sont gérés par Databricks, seul le fuseau est réglé.
    - path (local) : session construite avec les jars Delta et la configuration du projet.
    """
    if settings.storage.uses_catalog:
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return spark
    return build_spark_session(
        app_name=settings.spark.app_name,
        master=settings.spark.master,
        extra_conf={"spark.driver.memory": settings.spark.driver_memory},
    )
