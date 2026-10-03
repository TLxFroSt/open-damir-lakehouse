"""Tests du chargement silver d'un mois : séparation silver / quarantaine et contrôles."""

from collections.abc import Callable
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from damir.common.config import Settings
from damir.common.delta import overwrite_month
from damir.common.tables import TableRef
from damir.silver.job import (
    ReconciliationError,
    Totals,
    build_silver_month,
    check_reconciliation,
)
from damir.silver.transform import REJECTION_COLUMN

MakeBronze = Callable[..., DataFrame]


def load_bronze(settings: Settings, df: DataFrame, year_month: str = "202501") -> None:
    """Écrit un mois dans la table bronze de test."""
    overwrite_month(df, settings.bronze_table, year_month)


def month_rows(spark: SparkSession, table: TableRef, year_month: str = "202501") -> DataFrame:
    """Lignes d'un mois dans une table Delta."""
    return table.read(spark).where(F.col("_year_month") == year_month)


def test_valid_and_rejected_rows_are_split(
    spark: SparkSession, settings: Settings, make_bronze: MakeBronze
) -> None:
    """Lignes valides en silver, ligne invalide en quarantaine ; bilan cohérent avec le bronze."""
    load_bronze(settings, make_bronze({}, {}, {"PRS_PAI_MNT": "12,50"}))

    result = build_silver_month(spark, settings, 2025, 1)

    assert result.bronze == Totals(rows=3, amount=Decimal("1392.60"))
    assert result.silver == Totals(rows=2, amount=Decimal("1392.60"))
    # Montant non convertible : compte pour nul, des deux côtés de la comparaison
    assert result.quarantine == Totals(rows=1, amount=Decimal("0.00"))
    assert month_rows(spark, settings.silver_table).count() == 2


def test_quarantine_keeps_original_text_and_reasons(
    spark: SparkSession, settings: Settings, make_bronze: MakeBronze
) -> None:
    """La quarantaine garde la valeur d'origine en texte et la liste des motifs."""
    load_bronze(settings, make_bronze({"PRS_PAI_MNT": "12,50", "SOI_MOI": "13"}))

    build_silver_month(spark, settings, 2025, 1)

    row = month_rows(spark, settings.quarantine_table).first()
    assert row["PRS_PAI_MNT"] == "12,50"
    assert row[REJECTION_COLUMN] == ["non_numerique:PRS_PAI_MNT", "mois_soins_invalide"]


def test_rerun_after_fix_clears_old_rejections(
    spark: SparkSession, settings: Settings, make_bronze: MakeBronze
) -> None:
    """Mois corrigé puis rechargé : plus de rejet en quarantaine, tout en silver."""
    load_bronze(settings, make_bronze({}, {"PRS_PAI_MNT": "12,50"}))
    build_silver_month(spark, settings, 2025, 1)

    load_bronze(settings, make_bronze({}, {}))
    result = build_silver_month(spark, settings, 2025, 1)

    assert result.quarantine.rows == 0
    assert month_rows(spark, settings.quarantine_table).count() == 0
    assert month_rows(spark, settings.silver_table).count() == 2


def test_month_missing_from_bronze_is_reported(
    spark: SparkSession, settings: Settings, make_bronze: MakeBronze
) -> None:
    """Un mois absent du bronze produit une erreur explicite, sans rien écrire."""
    load_bronze(settings, make_bronze({}))

    with pytest.raises(ValueError, match="202502 absent du bronze"):
        build_silver_month(spark, settings, 2025, 2)


def test_reconciliation_accepts_matching_totals() -> None:
    """Silver + quarantaine = bronze : aucune erreur."""
    check_reconciliation(
        bronze=Totals(10, Decimal("100.00")),
        silver=Totals(8, Decimal("90.00")),
        quarantine=Totals(2, Decimal("10.00")),
    )


@pytest.mark.parametrize(
    ("silver", "message"),
    [
        (Totals(7, Decimal("90.00")), "Lignes"),
        (Totals(8, Decimal("89.99")), "Montant"),
    ],
)
def test_reconciliation_detects_losses(silver: Totals, message: str) -> None:
    """Une ligne ou un centime perdu fait échouer le contrôle."""
    with pytest.raises(ReconciliationError, match=message):
        check_reconciliation(
            bronze=Totals(10, Decimal("100.00")),
            silver=silver,
            quarantine=Totals(2, Decimal("10.00")),
        )
