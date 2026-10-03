"""Nomenclatures en silver : table Delta des libellés des codes DAMIR.

La table de faits garde les codes ; les libellés sont joints à la lecture (couche gold).
Les codes sans libellé sont signalés par le test dbt codes_sans_libelle (avertissement),
jamais rejetés.
"""

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from damir.common.delta import overwrite_table
from damir.common.tables import TableRef

NOMENCLATURES_SCHEMA = StructType(
    [
        StructField(name, StringType(), nullable=False)
        for name in ("code_damir", "code", "libelle", "source")
    ]
)


def read_nomenclatures(spark: SparkSession, csv_path: Path) -> DataFrame:
    """Lit reference/nomenclatures.csv avec un schéma explicite.

    Lecture en Python (quelques milliers de lignes) : fonctionne à l'identique en local et sur
    Databricks, où le fichier vient des fichiers du workspace et non d'un stockage Spark.
    """
    with csv_path.open(encoding="utf-8", newline="") as f:
        rows = [
            tuple(row[name] for name in NOMENCLATURES_SCHEMA.names)
            for row in csv.DictReader(f, delimiter=";")
        ]
    return spark.createDataFrame(rows, NOMENCLATURES_SCHEMA)


def write_nomenclatures(nomenclatures: DataFrame, table: TableRef) -> int:
    """Remplace entièrement la table Delta des nomenclatures et renvoie son nombre de lignes."""
    overwrite_table(nomenclatures, table)
    return nomenclatures.count()
