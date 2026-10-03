"""Test de fumée : la session Spark écrit et relit une table Delta."""

from pathlib import Path

from chispa import assert_df_equality
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


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
