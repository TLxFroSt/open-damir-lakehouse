"""Tests des nomenclatures en silver : table Delta et suivi des codes sans libellé."""

from collections.abc import Callable

from conftest import VALID_BRONZE_ROW
from pyspark.sql import DataFrame, SparkSession

from damir.common.config import Settings
from damir.common.delta import overwrite_month
from damir.reference.nomenclatures import OUTPUT_FILE
from damir.silver.job import build_silver_month
from damir.silver.nomenclatures import (
    known_codes,
    read_nomenclatures,
    unknown_code_rows,
    write_nomenclatures,
)
from damir.silver.transform import to_silver_columns

MakeBronze = Callable[..., DataFrame]

# Tous les codes de la ligne valide de référence ont un libellé...
ALL_KNOWN = {code: {value} for code, value in VALID_BRONZE_ROW.items()}
# ... sauf sa nature de prestation
WITHOUT_PRS_NAT = {**ALL_KNOWN, "PRS_NAT": set()}


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

    write_nomenclatures(df, settings.nomenclatures_table_path)
    rows = write_nomenclatures(df, settings.nomenclatures_table_path)

    assert rows == 1
    assert spark.read.format("delta").load(settings.nomenclatures_table_path).count() == 1


def test_known_codes_groups_by_damir_column(spark: SparkSession) -> None:
    """Codes connus regroupés par code DAMIR."""
    df = spark.createDataFrame(
        [("PRS_NAT", "1111", "a", "s"), ("PRS_NAT", "1112", "b", "s"), ("ASU_NAT", "10", "c", "s")],
        "code_damir STRING, code STRING, libelle STRING, source STRING",
    )

    assert known_codes(df) == {"PRS_NAT": {"1111", "1112"}, "ASU_NAT": {"10"}}


def test_no_unknown_code_when_every_code_has_a_label(make_bronze: MakeBronze) -> None:
    """Tous les codes connus : rien à signaler."""
    assert unknown_code_rows(to_silver_columns(make_bronze({}, {})), ALL_KNOWN) == {}


def test_unknown_codes_are_counted(make_bronze: MakeBronze) -> None:
    """Codes sans libellé comptés par valeur ; les autres colonnes ne sont pas signalées."""
    silver = to_silver_columns(make_bronze({}, {}, {"PRS_NAT": "9999"}))

    assert unknown_code_rows(silver, WITHOUT_PRS_NAT) == {"PRS_NAT": {"3134": 2, "9999": 1}}


def test_silver_job_reports_unknown_codes(
    spark: SparkSession, settings: Settings, make_bronze: MakeBronze
) -> None:
    """Le job silver renvoie les codes sans libellé du mois, sans rejeter de ligne."""
    overwrite_month(make_bronze({}, {}), settings.bronze_table_path, "202501")

    result = build_silver_month(spark, settings, 2025, 1, known_codes=WITHOUT_PRS_NAT)

    assert result.unknown_codes == {"PRS_NAT": {"3134": 2}}
    assert result.silver.rows == 2
    assert result.quarantine.rows == 0
