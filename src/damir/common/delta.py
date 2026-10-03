"""Écritures Delta partagées par les couches : remplacement d'un mois, lignes écrites."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

PARTITION_COLUMN = "_year_month"


def overwrite_month(df: DataFrame, table_path: str, year_month: str) -> None:
    """Écrit un mois dans une table Delta en remplaçant uniquement sa partition.

    Idempotence : relancer un mois remplace ses lignes au lieu de les ajouter, et un
    DataFrame vide vide la partition (utile pour la quarantaine d'un mois corrigé).
    Un MERGE ne conviendrait pas : les lignes DAMIR sont des agrégats sans clé
    unique, deux lignes identiques peuvent être légitimes.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{PARTITION_COLUMN} = '{year_month}'")
        .partitionBy(PARTITION_COLUMN)
        .save(table_path)
    )


def rows_written_by_last_commit(spark: SparkSession, table_path: str) -> int:
    """Nombre de lignes écrites par le dernier commit, lu dans l'historique Delta.

    Évite un count() qui relirait toute la partition.
    """
    last_commit = DeltaTable.forPath(spark, table_path).history(1).first()
    return int(last_commit["operationMetrics"]["numOutputRows"])
