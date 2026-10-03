"""Tests des transformations bronze -> silver."""

from collections.abc import Callable
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from damir.common.schemas import silver_schema
from damir.silver.transform import (
    REJECTION_COLUMN,
    parse_year_month,
    rejection_reasons,
    to_silver_columns,
)

MakeBronze = Callable[..., DataFrame]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("202501", date(2025, 1, 1)),
        ("202412", date(2024, 12, 1)),
        ("000000", None),  # mois des soins inconnu
        ("202513", None),  # mois impossible
        ("2025", None),  # incomplet
        ("abcdef", None),
    ],
)
def test_parse_year_month(spark: SparkSession, text: str, expected: date | None) -> None:
    """Année-mois valide -> 1er du mois ; tout le reste -> nul, sans erreur en mode ANSI."""
    df = spark.createDataFrame([(text,)], "v STRING")

    assert df.select(parse_year_month(F.col("v"))).first()[0] == expected


def test_to_silver_columns_matches_explicit_schema(make_bronze: MakeBronze) -> None:
    """Le résultat a exactement le schéma silver déclaré (noms, types, ordre)."""
    silver = to_silver_columns(make_bronze({}))

    assert [(f.name, f.dataType) for f in silver.schema] == [
        (f.name, f.dataType) for f in silver_schema()
    ]


def test_to_silver_columns_converts_values(make_bronze: MakeBronze) -> None:
    """Montants sans zéro initial, négatifs, entiers vides, dates : conversions attendues."""
    row = to_silver_columns(
        make_bronze(
            {
                "PRS_PAI_MNT": ".51",
                "PRS_REM_MNT": "-.31",
                "PRS_ACT_NBR": None,
                "SOI_ANN": "2024",
                "SOI_MOI": "02",
                "PRS_NAT": "0111",
            }
        )
    ).first()

    assert row["montant_depense"] == Decimal("0.51")
    assert row["montant_rembourse"] == Decimal("-0.31")
    assert row["denombrement_acte_base"] is None
    assert row["mois_soins"] == date(2024, 2, 1)
    assert row["mois_traitement"] == date(2025, 1, 1)
    assert row["nature_prestation"] == "0111"  # code : texte, zéro initial conservé
    assert row["taux_remboursement"] == Decimal("100.00")


@pytest.mark.parametrize(("year", "month"), [("0000", "00"), ("0001", "01")])
def test_unknown_care_date_is_kept_with_null(
    make_bronze: MakeBronze, year: str, month: str
) -> None:
    """Mois des soins inconnu (0000/00 ou 0001/01) : ligne valide, mois des soins nul."""
    df = rejection_reasons(make_bronze({"SOI_ANN": year, "SOI_MOI": month}))

    assert df.first()[REJECTION_COLUMN] == []
    assert to_silver_columns(df).first()["mois_soins"] is None


def test_valid_row_has_no_rejection_reason(make_bronze: MakeBronze) -> None:
    """Une vraie ligne DAMIR passe tous les contrôles."""
    assert rejection_reasons(make_bronze({})).first()[REJECTION_COLUMN] == []


@pytest.mark.parametrize(
    ("override", "expected_reasons"),
    [
        ({"PRS_PAI_MNT": "12,50"}, ["non_numerique:PRS_PAI_MNT"]),
        ({"PRS_ACT_QTE": "1.5"}, ["non_numerique:PRS_ACT_QTE"]),
        ({"PRS_REM_MNT": "1" * 20}, ["non_numerique:PRS_REM_MNT"]),  # dépasse decimal(18,2)
        ({"FLX_ANN_MOI": "202502"}, ["mois_traitement_incoherent"]),
        ({"SOI_MOI": "13"}, ["mois_soins_invalide"]),
        ({"SOI_ANN": "0001", "SOI_MOI": "02"}, ["mois_soins_invalide"]),
        ({"SOI_ANN": "2025", "SOI_MOI": "03"}, ["soins_apres_traitement"]),
        (
            {"PRS_PAI_MNT": "x", "FLT_PAI_MNT": "y"},
            ["non_numerique:PRS_PAI_MNT", "non_numerique:FLT_PAI_MNT"],
        ),
    ],
)
def test_rejection_reasons(
    make_bronze: MakeBronze, override: dict[str, str], expected_reasons: list[str]
) -> None:
    """Chaque anomalie produit son motif ; plusieurs anomalies, plusieurs motifs."""
    assert rejection_reasons(make_bronze(override)).first()[REJECTION_COLUMN] == expected_reasons
