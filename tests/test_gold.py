"""Test d'intégration de la couche gold : silver synthétique -> dbt build -> tables DuckDB."""

import os
from decimal import Decimal

import duckdb
import pytest
from conftest import bronze_rows, make_settings
from pyspark.sql import SparkSession

from damir.common.config import Settings
from damir.common.delta import overwrite_month
from damir.gold.build import GOLD_DB_ENV_VAR, dbt_build
from damir.silver.job import build_silver_month
from damir.silver.nomenclatures import write_nomenclatures

NOMENCLATURE_COLUMNS = "code_damir STRING, code STRING, libelle STRING, source STRING"


@pytest.fixture(scope="module")
def silver(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Settings:
    """Silver synthétique de janvier 2025 : deux prestations, deux régions, montants connus.

    Construit une seule fois pour le module (Spark est l'étape lente) ; chaque test réécrit
    ensuite sa table des nomenclatures et relance dbt.
    """
    settings = make_settings(tmp_path_factory.mktemp("gold"))
    bronze = bronze_rows(spark)(
        {"PRS_NAT": "1110", "BEN_RES_REG": "11", "PRS_PAI_MNT": "100", "PRS_REM_MNT": "70"},
        {"PRS_NAT": "1110", "BEN_RES_REG": "84", "PRS_PAI_MNT": "50", "PRS_REM_MNT": "35"},
        {"PRS_NAT": "3313", "BEN_RES_REG": "11", "PRS_PAI_MNT": "10", "PRS_REM_MNT": "6.5"},
    )
    overwrite_month(bronze, settings.bronze_table, "202501")
    build_silver_month(spark, settings, 2025, 1)
    return settings


def write_labels(spark: SparkSession, settings: Settings, rows: list[tuple[str, str, str]]) -> None:
    """Table des nomenclatures limitée aux libellés fournis."""
    df = spark.createDataFrame([(*row, "test") for row in rows], NOMENCLATURE_COLUMNS)
    write_nomenclatures(df, settings.nomenclatures_table)


ALL_LABELS = [
    ("PRS_NAT", "1110", "CONSULTATION MEDECINE GENERALE"),
    ("PRS_NAT", "3313", "PHARMACIE 65%"),
    ("BEN_RES_REG", "11", "ILE DE FRANCE"),
    ("BEN_RES_REG", "84", "AUVERGNE-RHONE-ALPES"),
]


def query(settings: Settings, sql: str) -> list[tuple]:
    """Requête sur le fichier DuckDB de la couche gold.

    Même configuration que la connexion de dbt-duckdb, restée ouverte dans ce processus :
    DuckDB refuse une seconde connexion au même fichier avec une configuration différente
    (read_only=True, par exemple).
    """
    with duckdb.connect(settings.paths.gold_db.as_posix()) as con:
        return con.sql(sql).fetchall()


def test_gold_tables_aggregate_and_decode(spark: SparkSession, silver: Settings) -> None:
    """dbt build réussit ; agrégats, libellés et taux attendus."""
    write_labels(spark, silver, ALL_LABELS)

    assert dbt_build(silver)

    assert query(
        silver,
        "select nature_prestation, libelle_nature_prestation, nombre_lignes, montant_depense,"
        " montant_rembourse, taux_remboursement_effectif"
        " from depenses_par_prestation order by nature_prestation",
    ) == [
        # 150 € dépensés, 105 € remboursés : taux 70 %
        ("1110", "CONSULTATION MEDECINE GENERALE", 2, Decimal("150.00"), Decimal("105.00"),
         Decimal("0.7000")),
        ("3313", "PHARMACIE 65%", 1, Decimal("10.00"), Decimal("6.50"), Decimal("0.6500")),
    ]  # fmt: skip
    assert query(
        silver,
        "select region_residence_beneficiaire, montant_depense, part_depense_nationale"
        " from depenses_par_region order by region_residence_beneficiaire",
    ) == [("11", Decimal("110.00"), Decimal("0.6875")), ("84", Decimal("50.00"), Decimal("0.3125"))]


def test_missing_label_warns_without_failing(spark: SparkSession, silver: Settings) -> None:
    """Code sans libellé : la construction réussit, le libellé est nul, la ligne est gardée."""
    write_labels(spark, silver, [label for label in ALL_LABELS if label[1] != "84"])

    assert dbt_build(silver)

    assert query(
        silver,
        "select libelle_region_residence_beneficiaire, montant_depense"
        " from depenses_par_region where region_residence_beneficiaire = '84'",
    ) == [(None, Decimal("50.00"))]


def test_dbt_build_restores_environment(
    spark: SparkSession, silver: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La variable lue par le profil dbt est rétablie après la construction."""
    monkeypatch.setenv(GOLD_DB_ENV_VAR, "valeur-precedente")
    write_labels(spark, silver, ALL_LABELS)

    dbt_build(silver)

    assert os.environ[GOLD_DB_ENV_VAR] == "valeur-precedente"
