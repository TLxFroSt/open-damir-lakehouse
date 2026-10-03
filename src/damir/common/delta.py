"""Écritures Delta partagées par les couches (tables par chemin ou Unity Catalog)."""

from pyspark.sql import DataFrame, SparkSession

from damir.common.tables import TableRef

PARTITION_COLUMN = "_year_month"


def overwrite_month(df: DataFrame, table: TableRef, year_month: str) -> None:
    """Écrit un mois dans une table Delta en remplaçant uniquement sa partition.

    Idempotence : relancer un mois remplace ses lignes au lieu de les ajouter, et un
    DataFrame vide vide la partition (utile pour la quarantaine d'un mois corrigé).
    Un MERGE ne conviendrait pas : les lignes DAMIR sont des agrégats sans clé
    unique, deux lignes identiques peuvent être légitimes.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{PARTITION_COLUMN} = '{year_month}'")
        .partitionBy(PARTITION_COLUMN)
    )
    table.save(writer)


def overwrite_table(df: DataFrame, table: TableRef) -> None:
    """Remplace entièrement une table Delta (petites tables de référence)."""
    table.save(df.write.format("delta").mode("overwrite"))


def rows_written_by_last_commit(spark: SparkSession, table: TableRef) -> int:
    """Nombre de lignes écrites par le dernier commit, lu dans l'historique Delta.

    Évite un count() qui relirait toute la partition. DESCRIBE HISTORY fonctionne en local
    comme sur Databricks (Spark Connect), contrairement à l'API Python DeltaTable.
    """
    last_commit = spark.sql(f"DESCRIBE HISTORY {table.sql_name} LIMIT 1").first()
    return int(last_commit["operationMetrics"]["numOutputRows"])
