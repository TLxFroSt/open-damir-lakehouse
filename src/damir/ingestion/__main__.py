"""Ligne de commande : uv run python -m damir.ingestion --year 2025 --month 1 [2 ...]"""

import argparse
import logging

from damir.common.cli import configure_logging, parse_month_args, spark_from_settings
from damir.common.config import load_settings
from damir.ingestion.bronze import check_raw_files, ingest_months

# Nom explicite : lancé avec -m, __name__ vaut « __main__ », hors de la hiérarchie « damir »
# réglée en INFO (le récapitulatif final serait alors filtré)
logger = logging.getLogger("damir.ingestion")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Lit les arguments de la ligne de commande."""
    return parse_month_args("Charge des mois Open DAMIR en couche bronze.", argv)


def main(argv: list[str] | None = None) -> None:
    """Charge les mois demandés dans la table bronze, avec une seule session Spark."""
    args = parse_args(argv)
    configure_logging()
    settings = load_settings(args.config)

    # Vérification avant le démarrage de Spark : une erreur d'usage s'affiche sans
    # attendre la JVM, et sans trace Python
    try:
        check_raw_files(settings, args.year, args.month)
    except (FileNotFoundError, ValueError) as error:
        logger.error("%s", error)
        raise SystemExit(1) from None

    spark = spark_from_settings(settings)
    try:
        rows_by_month = ingest_months(spark, settings, args.year, args.month)
    finally:
        spark.stop()
    logger.info(
        "Terminé : %s lignes sur %d mois", f"{sum(rows_by_month.values()):,}", len(rows_by_month)
    )


if __name__ == "__main__":
    main()
