"""Tests de la ligne de commande d'ingestion."""

from pathlib import Path

import pytest
import yaml

from damir.ingestion.__main__ import main, parse_args


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """Fichier de config pointant vers un dossier raw vide."""
    content = {
        "paths": {"raw_dir": "raw", "bronze_dir": "bronze"},
        "source": {
            "file_name_template": "A{year}{month:02d}.csv.gz",
            "csv_separator": ";",
            "csv_encoding": "UTF-8",
        },
        "spark": {"master": "local[1]", "app_name": "test", "driver_memory": "1g"},
        "bronze": {"table_name": "open_damir", "files_per_month": 1},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return path


def test_single_month() -> None:
    """Un seul mois donne une liste d'un élément."""
    args = parse_args(["--year", "2025", "--month", "1"])

    assert args.year == 2025
    assert args.month == [1]
    assert args.config is None


def test_several_months() -> None:
    """--month accepte plusieurs valeurs."""
    assert parse_args(["--year", "2025", "--month", "1", "2", "3"]).month == [1, 2, 3]


def test_month_is_required() -> None:
    """Sans --month, argparse s'arrête avec une erreur d'usage."""
    with pytest.raises(SystemExit):
        parse_args(["--year", "2025"])


@pytest.mark.parametrize("month", ["3", "13"])
def test_usage_error_exits_before_spark(
    config_path: Path, month: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Fichier absent ou mois invalide : code de sortie 1 et message clair, sans démarrer Spark."""
    with pytest.raises(SystemExit) as exit_info:
        main(["--year", "2025", "--month", month, "--config", str(config_path)])

    assert exit_info.value.code == 1
    assert caplog.records[-1].levelname == "ERROR"
    # Logger dans la hiérarchie « damir » (réglée en INFO), pas « __main__ »
    assert caplog.records[-1].name == "damir.ingestion"
