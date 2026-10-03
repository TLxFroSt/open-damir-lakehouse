"""Cohérence entre le projet dbt et le schéma silver déclaré en Python."""

import re
from pathlib import Path

from pyspark.sql.types import StringType

from damir.common.schemas import SILVER_COLUMNS

UNKNOWN_CODES_TEST = Path(__file__).parents[1] / "dbt" / "tests" / "codes_sans_libelle.sql"


def test_unknown_codes_test_covers_every_code_column() -> None:
    """Le test dbt des codes sans libellé vise exactement les colonnes de codes du silver."""
    sql = UNKNOWN_CODES_TEST.read_text(encoding="utf-8")
    block = sql[sql.index("colonnes_de_codes = [") : sql.index("] %}")]
    declared = re.findall(r"\('(\w+)', '(\w+)'\)", block)
    expected = [(s.name, s.sources[0]) for s in SILVER_COLUMNS if isinstance(s.dtype, StringType)]

    assert declared == expected
