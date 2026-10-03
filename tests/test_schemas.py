"""Tests du schéma silver et de sa documentation générée."""

import re
from pathlib import Path

from pyspark.sql.types import DecimalType

from damir.common.schemas import (
    AMOUNT,
    DAMIR_SOURCE_COLUMNS,
    SILVER_COLUMNS,
    columns_markdown,
    silver_schema,
)

DOC_PATH = Path(__file__).parents[1] / "docs" / "colonnes_silver.md"


def test_every_damir_column_is_mapped_exactly_once() -> None:
    """Chaque colonne des fichiers DAMIR alimente une et une seule colonne silver."""
    sources = [code for spec in SILVER_COLUMNS for code in spec.sources]

    assert sorted(sources) == sorted(DAMIR_SOURCE_COLUMNS)


def test_silver_names_are_unique_snake_case() -> None:
    """Noms silver uniques, en snake_case sans accent."""
    names = [spec.name for spec in SILVER_COLUMNS]

    assert len(names) == len(set(names))
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in names)


def test_amounts_are_decimal_18_2() -> None:
    """Tous les montants (codes *_MNT et base de remboursement) sont en decimal(18,2)."""
    amount_specs = [s for s in SILVER_COLUMNS if s.sources[0].endswith(("_MNT", "_BSE"))]

    assert len(amount_specs) == 7
    assert all(spec.dtype == AMOUNT == DecimalType(18, 2) for spec in amount_specs)


def test_schema_ends_with_technical_columns() -> None:
    """Le schéma Spark contient les colonnes métier puis les trois colonnes techniques."""
    names = silver_schema().fieldNames()

    assert names[: len(SILVER_COLUMNS)] == [spec.name for spec in SILVER_COLUMNS]
    assert names[len(SILVER_COLUMNS) :] == ["_source_file", "_ingested_at", "_year_month"]


def test_generated_doc_is_up_to_date() -> None:
    """docs/colonnes_silver.md correspond au schéma : sinon, la régénérer."""
    assert DOC_PATH.read_text(encoding="utf-8") == columns_markdown(), (
        "Doc obsolète : uv run python -m damir.common.schemas docs/colonnes_silver.md"
    )
