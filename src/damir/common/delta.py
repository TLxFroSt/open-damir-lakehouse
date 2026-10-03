"""Écritures Delta partagées par les couches (tables par chemin ou Unity Catalog)."""

from collections.abc import Mapping

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from damir.common.tables import TableRef

PARTITION_COLUMN = "_year_month"


def table_exists(spark: SparkSession, table: TableRef) -> bool:
    """La table Delta existe-t-elle déjà ?"""
    try:
        spark.sql(f"DESCRIBE DETAIL {table.sql_name}").first()
    except AnalysisException:
        return False
    return True


def overwrite_month(
    df: DataFrame,
    table: TableRef,
    year_month: str,
    table_properties: Mapping[str, str] | None = None,
) -> None:
    """Écrit un mois dans une table Delta en remplaçant uniquement sa partition.

    Idempotence : relancer un mois remplace ses lignes au lieu de les ajouter, et un
    DataFrame vide vide la partition (utile pour la quarantaine d'un mois corrigé).
    Un MERGE ne conviendrait pas : les lignes DAMIR sont des agrégats sans clé
    unique, deux lignes identiques peuvent être légitimes.

    `table_properties` (propriétés Delta) : options d'écriture si la table est créée, ALTER TABLE
    avant l'écriture si elle existe déjà (Delta n'applique les options qu'à la création).
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{PARTITION_COLUMN} = '{year_month}'")
        .partitionBy(PARTITION_COLUMN)
    )
    if table_properties:
        spark = df.sparkSession
        if table_exists(spark, table):
            assignments = ", ".join(
                f"'{key}' = '{value}'" for key, value in table_properties.items()
            )
            spark.sql(f"ALTER TABLE {table.sql_name} SET TBLPROPERTIES ({assignments})")
        else:
            for key, value in table_properties.items():
                writer = writer.option(key, value)
    table.save(writer)


def overwrite_table(df: DataFrame, table: TableRef) -> None:
    """Remplace entièrement une table Delta (petites tables de référence)."""
    table.save(df.write.format("delta").mode("overwrite"))


def table_properties(spark: SparkSession, table: TableRef) -> dict[str, str]:
    """Propriétés Delta de la table (DESCRIBE DETAIL)."""
    return dict(spark.sql(f"DESCRIBE DETAIL {table.sql_name}").first()["properties"])


def rows_written_by_last_commit(spark: SparkSession, table: TableRef) -> int:
    """Nombre de lignes écrites par le dernier commit, lu dans l'historique Delta.

    Évite un count() qui relirait toute la partition. DESCRIBE HISTORY fonctionne en local
    comme sur Databricks (Spark Connect), contrairement à l'API Python DeltaTable.
    """
    last_commit = spark.sql(f"DESCRIBE HISTORY {table.sql_name} LIMIT 1").first()
    return int(last_commit["operationMetrics"]["numOutputRows"])
