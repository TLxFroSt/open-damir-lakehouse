"""Tests de la session Spark : configuration et aller-retour Delta."""

from pathlib import Path

from chispa import assert_df_equality
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from damir.common.spark import LOG4J_CONFIG


def test_session_uses_project_log_config(spark: SparkSession) -> None:
    """La JVM du driver charge le log4j2.properties du projet."""
    system = spark.sparkContext._jvm.java.lang.System

    assert LOG4J_CONFIG.is_file()
    assert system.getProperty("log4j2.configurationFile") == LOG4J_CONFIG.as_uri()


def test_delta_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    """Une table écrite en Delta se relit à l'identique, avec un historique versionné."""
    table_path = (tmp_path / "smoke").as_posix()
    expected = spark.createDataFrame(
        [(1, "pharmacie"), (2, "consultation")],
        "id INT, prestation STRING",
    )

    expected.write.format("delta").save(table_path)
    actual = spark.read.format("delta").load(table_path)

    assert_df_equality(actual, expected, ignore_row_order=True)
    assert DeltaTable.isDeltaTable(spark, table_path)
    assert DeltaTable.forPath(spark, table_path).history().count() == 1
