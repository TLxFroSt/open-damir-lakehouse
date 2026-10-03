"""Ligne de commande : uv run python -m damir.silver --year 2025 --month 1 [2 ...]"""

import argparse
import logging
from pathlib import Path

from damir.common.cli import (
    check_months,
    configure_logging,
    parse_month_args,
    spark_from_settings,
)
from damir.common.config import load_settings
from damir.silver.job import build_silver_month
from damir.silver.nomenclatures import known_codes, read_nomenclatures, write_nomenclatures

# Nom explicite : lancé avec -m, __name__ vaut « __main__ », hors de la hiérarchie « damir »
logger = logging.getLogger("damir.silver")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Lit les arguments de la ligne de commande."""
    return parse_month_args("Construit le silver de mois déjà chargés en bronze.", argv)


def main(argv: list[str] | None = None) -> None:
    """Construit le silver des mois demandés, avec une seule session Spark."""
    args = parse_args(argv)
    configure_logging()
    settings = load_settings(args.config)

    # Vérifications avant le démarrage de Spark (erreur d'usage immédiate, sans trace Python)
    try:
        check_months(args.month)
        if not Path(settings.bronze_table_path, "_delta_log").is_dir():
            raise FileNotFoundError(f"Table bronze introuvable : {settings.bronze_table_path}")
        if not settings.paths.nomenclatures_file.is_file():
            raise FileNotFoundError(
                f"Fichier de nomenclatures introuvable : {settings.paths.nomenclatures_file}"
            )
    except (FileNotFoundError, ValueError) as error:
        logger.error("%s", error)
        raise SystemExit(1) from None

    spark = spark_from_settings(settings)
    try:
        # Table des libellés rechargée depuis le fichier versionné, une fois par exécution
        nomenclatures = read_nomenclatures(spark, settings.paths.nomenclatures_file)
        rows = write_nomenclatures(nomenclatures, settings.nomenclatures_table_path)
        logger.info("Nomenclatures : %s libellés chargés", f"{rows:,}")
        known = known_codes(nomenclatures)
        results = [build_silver_month(spark, settings, args.year, m, known) for m in args.month]
    finally:
        spark.stop()
    logger.info(
        "Terminé : %s lignes en silver, %s en quarantaine, sur %d mois",
        f"{sum(r.silver.rows for r in results):,}",
        f"{sum(r.quarantine.rows for r in results):,}",
        len(results),
    )


if __name__ == "__main__":
    main()
