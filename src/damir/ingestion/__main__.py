"""Ligne de commande : uv run python -m damir.ingestion --year 2025 --month 1 [2 ...]"""

import argparse
import logging
from pathlib import Path

from damir.common.config import load_settings
from damir.common.spark import build_spark_session
from damir.ingestion.bronze import check_raw_files, ingest_months

# Nom explicite : lancé avec -m, __name__ vaut « __main__ », hors de la hiérarchie « damir »
# réglée en INFO (le récapitulatif final serait alors filtré)
logger = logging.getLogger("damir.ingestion")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Lit les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(description="Charge des mois Open DAMIR en couche bronze.")
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


def main(argv: list[str] | None = None) -> None:
    """Charge les mois demandés dans la table bronze, avec une seule session Spark."""
    args = parse_args(argv)
    # INFO pour les messages du projet uniquement ; les bibliothèques (py4j...) restent en WARNING
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )
    logging.getLogger("damir").setLevel(logging.INFO)
    settings = load_settings(args.config)

    # Vérification avant le démarrage de Spark : une erreur d'usage s'affiche sans
    # attendre la JVM, et sans trace Python
    try:
        check_raw_files(settings, args.year, args.month)
    except (FileNotFoundError, ValueError) as error:
        logger.error("%s", error)
        raise SystemExit(1) from None

    spark = build_spark_session(
        app_name=settings.spark.app_name,
        master=settings.spark.master,
        extra_conf={"spark.driver.memory": settings.spark.driver_memory},
    )
    try:
        rows_by_month = ingest_months(spark, settings, args.year, args.month)
    finally:
        spark.stop()
    logger.info(
        "Terminé : %s lignes sur %d mois", f"{sum(rows_by_month.values()):,}", len(rows_by_month)
    )


if __name__ == "__main__":
    main()
