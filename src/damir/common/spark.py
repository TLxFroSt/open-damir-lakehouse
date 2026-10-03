"""Création de la session Spark locale configurée pour Delta Lake."""

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

LOG4J_CONFIG = Path(__file__).with_name("log4j2.properties")


def build_spark_session(
    app_name: str = "open-damir-lakehouse",
    master: str = "local[*]",
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Construit (ou récupère) une session Spark locale avec Delta Lake activé.

    Les jars Delta sont téléchargés depuis Maven au premier lancement
    (puis mis en cache dans ~/.ivy2) par `configure_spark_with_delta_pip`.
    """
    # Les workers Python doivent utiliser l'interpréteur du driver (le venv).
    # Sans cela, PySpark lance « python3 », qui sous Windows pointe vers l'alias
    # du Microsoft Store (autre version de Python : erreur PYTHON_VERSION_MISMATCH).
    # PySpark ne lit que la variable d'environnement, pas spark.pyspark.python.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Horodatages en UTC, quel que soit le fuseau de la machine
        .config("spark.sql.session.timeZone", "UTC")
        # Journalisation du projet (niveau WARN, bruits connus masqués) au lieu du profil par défaut
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile={LOG4J_CONFIG.as_uri()}",
        )
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()
