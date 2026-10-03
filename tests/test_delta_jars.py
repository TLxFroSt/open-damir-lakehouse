"""Tests de la gestion des jars Delta (sans téléchargement)."""

import re
from pathlib import Path

import pytest

from damir.common import delta_jars
from damir.common.delta_jars import delta_jar_uris, delta_maven_coordinate


def test_coordinate_matches_installed_versions() -> None:
    """La coordonnée suit le format io.delta:delta-spark_<spark mineur>_2.13:<delta>."""
    assert re.fullmatch(
        r"io\.delta:delta-spark_\d+\.\d+_2\.13:\d+\.\d+\.\d+", delta_maven_coordinate()
    )


def fake_cache(cache_dir: Path) -> Path:
    """Crée un cache déjà complet (deux jars et le marqueur) pour la coordonnée installée."""
    target = cache_dir / delta_maven_coordinate().replace(":", "_")
    (target / "jars").mkdir(parents=True)
    for name in ("b-dep.jar", "a-delta.jar"):
        (target / "jars" / name).write_bytes(b"")
    (target / "COMPLETE").write_text("ok", encoding="utf-8")
    return target


def test_complete_cache_is_reused_without_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache complet : URI « local: » triées, aucun téléchargement."""
    target = fake_cache(tmp_path)

    def fail_download(*args: object) -> None:
        raise AssertionError("aucun téléchargement attendu")

    monkeypatch.setattr(delta_jars, "_download_with_spark_submit", fail_download)

    uris = delta_jar_uris(tmp_path)

    assert uris == [
        "local:" + (target / "jars" / name).as_uri().removeprefix("file:")
        for name in ("a-delta.jar", "b-dep.jar")
    ]
    assert all(uri.startswith("local:///") for uri in uris)


def test_incomplete_cache_is_downloaded_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sans marqueur (téléchargement interrompu), le dossier est vidé puis retéléchargé."""
    target = fake_cache(tmp_path)
    (target / "COMPLETE").unlink()
    downloads: list[str] = []

    def fake_download(coordinate: str, target_dir: Path) -> None:
        assert not target_dir.exists(), "le cache incomplet doit être supprimé avant"
        downloads.append(coordinate)
        (target_dir / "jars").mkdir(parents=True)
        (target_dir / "jars" / "delta.jar").write_bytes(b"")

    monkeypatch.setattr(delta_jars, "_download_with_spark_submit", fake_download)

    uris = delta_jar_uris(tmp_path)

    assert downloads == [delta_maven_coordinate()]
    assert len(uris) == 1
    assert (target / "COMPLETE").is_file()
