"""Nomenclatures en silver : table Delta des libellés et suivi des codes sans libellé.

La table de faits garde les codes ; les libellés sont joints à la lecture (couche gold).
Un code sans libellé n'est jamais un motif de rejet : il est signalé dans les logs.
"""

import csv
from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from damir.common.delta import overwrite_table
from damir.common.schemas import SILVER_COLUMNS
from damir.common.tables import TableRef

NOMENCLATURES_SCHEMA = StructType(
    [
        StructField(name, StringType(), nullable=False)
        for name in ("code_damir", "code", "libelle", "source")
    ]
)

# Colonnes de codes du silver : (nom silver, code DAMIR)
CODE_COLUMNS = [(s.name, s.sources[0]) for s in SILVER_COLUMNS if isinstance(s.dtype, StringType)]


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


def known_codes(nomenclatures: DataFrame) -> dict[str, set[str]]:
    """Codes ayant un libellé, par code DAMIR (quelques milliers de lignes : collectées)."""
    known: dict[str, set[str]] = {}
    for row in nomenclatures.select("code_damir", "code").collect():
        known.setdefault(row["code_damir"], set()).add(row["code"])
    return known


def unknown_code_rows(
    silver_month: DataFrame, known: Mapping[str, set[str]]
) -> dict[str, dict[str, int]]:
    """Codes sans libellé et leur nombre de lignes, par code DAMIR : {"PRS_NAT": {"9999": 12}}.

    Deux lectures au plus : les valeurs distinctes de chaque colonne de codes, puis, s'il
    y a des codes inconnus, leurs nombres de lignes. Les valeurs nulles sont ignorées.
    """
    distinct = silver_month.agg(
        *[F.collect_set(name).alias(name) for name, _ in CODE_COLUMNS]
    ).first()
    unknown = [
        (name, code_damir, code)
        for name, code_damir in CODE_COLUMNS
        for code in sorted(distinct[name])
        if code not in known.get(code_damir, set())
    ]
    if not unknown:
        return {}

    counts = silver_month.agg(
        *[
            F.sum(F.when(F.col(name) == code, 1).otherwise(0)).alias(f"u{i}")
            for i, (name, _, code) in enumerate(unknown)
        ]
    ).first()
    result: dict[str, dict[str, int]] = {}
    for i, (_, code_damir, code) in enumerate(unknown):
        result.setdefault(code_damir, {})[code] = counts[f"u{i}"]
    return result
