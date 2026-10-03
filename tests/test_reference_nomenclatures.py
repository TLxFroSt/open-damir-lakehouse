"""Tests de la construction des nomenclatures et du fichier de référence généré."""

import csv

import pytest
from pyspark.sql.types import StringType

from damir.common.schemas import SILVER_COLUMNS
from damir.reference.nomenclatures import (
    COLUMN_SOURCES,
    COMPLEMENT_SOURCE,
    OUTPUT_FILE,
    LexiconSheet,
    SndsTable,
    build_rows,
    merge_column,
    normalize_code,
    parse_snds_table,
    read_complements,
)

CODE_COLUMNS = {s.sources[0] for s in SILVER_COLUMNS if isinstance(s.dtype, StringType)}


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1111.0", "1111"), (1111.0, "1111"), (" 11 ", "11"), ("Z", "Z"), ("0", "0")],
)
def test_normalize_code(value: object, expected: str) -> None:
    """Les codes lus dans Excel (« 1111.0 ») et dans les CSV se comparent sous la même forme."""
    assert normalize_code(value) == expected


def test_parse_snds_table_keeps_first_label_and_cleans_quotes() -> None:
    """Table SNDS : code et libellé choisis, guillemets retirés, premier code répété gardé."""
    content = 'COD;AUTRE;LIB\n10;x;"""MALADIE"""\n10;x;DOUBLON\n;x;SANS CODE\n'

    assert parse_snds_table(content, SndsTable("T", "COD", "LIB")) == {"10": "MALADIE"}


def test_merge_column_follows_priority_then_complements() -> None:
    """Première source prioritaire ; la seconde complète ; les compléments comblent le reste."""
    rows = merge_column(
        "COL",
        [("A", {"1": "un (A)", "2": "deux (A)"}), ("B", {"2": "deux (B)", "3": "trois (B)"})],
        complements={"4": "quatre"},
    )

    assert rows == [
        ("COL", "1", "un (A)", "A"),
        ("COL", "2", "deux (A)", "A"),
        ("COL", "3", "trois (B)", "B"),
        ("COL", "4", "quatre", COMPLEMENT_SOURCE),
    ]


def test_complement_duplicating_a_source_is_rejected() -> None:
    """Un complément pour un code déjà couvert est une erreur."""
    with pytest.raises(ValueError, match="déjà couverts"):
        merge_column("COL", [("A", {"1": "un"})], complements={"1": "autre"})


def test_build_rows_rejects_complements_for_unknown_columns() -> None:
    """Un complément pour une colonne absente de COLUMN_SOURCES est une erreur."""
    with pytest.raises(ValueError, match="colonnes inconnues"):
        build_rows(lambda t: {}, lambda s: {}, {"PAS_UNE_COLONNE": {"1": "x"}})


def test_every_code_column_has_a_declared_source() -> None:
    """Chaque colonne de codes du silver a une entrée (éventuellement vide) dans COLUMN_SOURCES."""
    assert set(COLUMN_SOURCES) == CODE_COLUMNS


def test_age_bands_never_use_the_snds_age_table() -> None:
    """Garde-fou : les tranches d'âge Open DAMIR ne viennent que du lexique (voir docstring)."""
    assert COLUMN_SOURCES["AGE_BEN_SNDS"] == [LexiconSheet("AGE_BEN_SNDS")]


def test_committed_complements_are_valid() -> None:
    """Le fichier de compléments se lit et ne vise que des colonnes connues."""
    assert set(read_complements()) <= set(COLUMN_SOURCES)


def test_committed_nomenclatures_file_is_consistent() -> None:
    """Fichier généré : toutes les colonnes présentes, aucun code en double, libellés non vides."""
    with OUTPUT_FILE.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    keys = [(r["code_damir"], r["code"]) for r in rows]

    assert {r["code_damir"] for r in rows} == CODE_COLUMNS
    assert len(keys) == len(set(keys))
    assert all(r["libelle"].strip() and r["source"].strip() for r in rows)
