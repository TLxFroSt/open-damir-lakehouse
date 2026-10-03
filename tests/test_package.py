"""Vérifie que le paquet et ses sous-paquets s'importent."""

import importlib

import pytest


@pytest.mark.parametrize(
    "module",
    ["damir", "damir.ingestion", "damir.silver", "damir.common"],
)
def test_import(module: str) -> None:
    """Chaque sous-paquet du squelette doit être importable."""
    importlib.import_module(module)
