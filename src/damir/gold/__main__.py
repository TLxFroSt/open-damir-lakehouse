"""Ligne de commande : uv run python -m damir.gold"""

import argparse
import logging
from pathlib import Path

from damir.common.cli import configure_logging
from damir.common.config import load_settings
from damir.gold.build import dbt_build

# Nom explicite : lancé avec -m, __name__ vaut « __main__ », hors de la hiérarchie « damir »
logger = logging.getLogger("damir.gold")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Lit les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(description="Construit la couche gold avec dbt.")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="fichier de configuration (défaut : variable DAMIR_CONFIG, sinon ./config.yaml)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Construit les tables gold et lance leurs tests (dbt build)."""
    args = parse_args(argv)
    configure_logging()
    settings = load_settings(args.config)

    # Tables silver présentes avant de lancer dbt (erreur immédiate et lisible)
    missing = [
        path
        for path in (settings.silver_table_path, settings.nomenclatures_table_path)
        if not Path(path, "_delta_log").is_dir()
    ]
    if missing:
        logger.error("Table(s) silver introuvable(s) : %s", ", ".join(missing))
        raise SystemExit(1)

    if not dbt_build(settings):
        logger.error("dbt build a échoué : voir le détail ci-dessus")
        raise SystemExit(1)
    logger.info("Couche gold construite : %s", settings.paths.gold_db)


if __name__ == "__main__":
    main()
