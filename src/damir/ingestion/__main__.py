"""Ligne de commande : uv run python -m damir.ingestion --year 2025 --month 1"""

import argparse
import logging
from pathlib import Path

from damir.common.config import load_settings
from damir.common.spark import build_spark_session
from damir.ingestion.bronze import ingest_month


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Lit les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(description="Charge un mois Open DAMIR en couche bronze.")
    parser.add_argument("--year", type=int, required=True, help="année, par exemple 2025")
    parser.add_argument("--month", type=int, required=True, help="mois de 1 à 12")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="fichier de configuration (défaut : variable DAMIR_CONFIG, sinon ./config.yaml)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Charge le mois demandé dans la table bronze."""
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )
    settings = load_settings(args.config)

    spark = build_spark_session(
        app_name=settings.spark.app_name,
        master=settings.spark.master,
        extra_conf={"spark.driver.memory": settings.spark.driver_memory},
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        ingest_month(spark, settings, args.year, args.month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
