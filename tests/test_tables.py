"""Tests des références de tables Delta."""

from damir.common.tables import TableRef


def test_sql_name_of_a_path_table() -> None:
    """Table par chemin : syntaxe delta.`chemin` en SQL."""
    assert TableRef("C:/data/bronze/open_damir", is_path=True).sql_name == (
        "delta.`C:/data/bronze/open_damir`"
    )


def test_sql_name_of_a_catalog_table() -> None:
    """Table Unity Catalog : son nom complet."""
    assert TableRef("workspace.silver.open_damir", is_path=False).sql_name == (
        "workspace.silver.open_damir"
    )
