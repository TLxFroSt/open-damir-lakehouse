"""Tests des nomenclatures en silver : lecture du fichier versionné et table Delta."""

from pyspark.sql import SparkSession

from damir.common.config import Settings
from damir.reference.nomenclatures import OUTPUT_FILE
from damir.silver.nomenclatures import read_nomenclatures, write_nomenclatures


def test_committed_file_loads_with_explicit_schema(spark: SparkSession) -> None:
    """Le fichier versionné se lit entièrement, colonnes attendues, toutes en texte."""
    df = read_nomenclatures(spark, OUTPUT_FILE)
    expected_rows = len(OUTPUT_FILE.read_text(encoding="utf-8").splitlines()) - 1

    assert df.columns == ["code_damir", "code", "libelle", "source"]
    assert {dtype for _, dtype in df.dtypes} == {"string"}
    assert df.count() == expected_rows


def test_write_nomenclatures_replaces_the_table(spark: SparkSession, settings: Settings) -> None:
    """Réécrire la table la remplace (pas d'accumulation d'une exécution à l'autre)."""
    df = spark.createDataFrame(
        [("PRS_NAT", "1111", "CONSULTATION", "test")],
        "code_damir STRING, code STRING, libelle STRING, source STRING",
    )

    write_nomenclatures(df, settings.nomenclatures_table)
    rows = write_nomenclatures(df, settings.nomenclatures_table)

    assert rows == 1
    assert settings.nomenclatures_table.read(spark).count() == 1
