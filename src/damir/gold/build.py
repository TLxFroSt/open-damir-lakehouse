"""Lancement de dbt avec les chemins de config.yaml (une seule source de configuration)."""

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from damir.common.config import Settings

DBT_PROJECT_DIR = Path(__file__).parents[3] / "dbt"

# Lu par dbt/profiles.yml : fichier DuckDB de la couche gold
GOLD_DB_ENV_VAR = "DAMIR_GOLD_DB"


@contextmanager
def _env_var(name: str, value: str) -> Iterator[None]:
    """Définit une variable d'environnement le temps du bloc, puis restaure l'ancienne valeur."""
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            del os.environ[name]
        else:
            os.environ[name] = previous


def dbt_vars(settings: Settings) -> dict[str, str]:
    """Variables dbt : emplacements des tables silver lues par les sources."""
    return {
        "silver_table_path": settings.silver_table_path,
        "nomenclatures_table_path": settings.nomenclatures_table_path,
    }


def dbt_build(settings: Settings) -> bool:
    """Exécute « dbt build » (modèles et tests) et renvoie True si tout a réussi.

    Les tests en avertissement (codes sans libellé) ne font pas échouer la construction.
    """
    # Import différé : importer dbt ajoute un gestionnaire de logs au format par défaut sur la
    # racine, ce qui rendrait sans effet la configuration faite ensuite par la ligne de commande
    from dbt.cli.main import dbtRunner

    settings.paths.gold_db.parent.mkdir(parents=True, exist_ok=True)
    args = [
        "build",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
        "--vars", json.dumps(dbt_vars(settings)),
    ]  # fmt: skip
    with _env_var(GOLD_DB_ENV_VAR, settings.paths.gold_db.as_posix()):
        return dbtRunner().invoke(args).success
