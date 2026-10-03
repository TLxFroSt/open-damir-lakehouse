"""Tests de la couche bronze, sur de petits fichiers DAMIR synthétiques."""

import gzip
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from damir.common.config import Settings
from damir.common.delta import overwrite_month, table_properties
from damir.ingestion.bronze import (
    BRONZE_TABLE_PROPERTIES,
    add_technical_columns,
    decompressed,
    drop_unnamed_columns,
    ingest_month,
    ingest_months,
    read_raw_csv,
)

# Comme dans les vrais fichiers : séparateur « ; » et « ; » final sur chaque ligne
HEADER = "FLX_ANN_MOI;PRS_NAT;PRS_PAI_MNT;"


def write_damir_file(path: Path, rows: list[str]) -> None:
    """Écrit un fichier DAMIR gzip minimal : en-tête puis une ligne par élément de `rows`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
        f.write(HEADER + "\n")
        for row in rows:
            f.write(row + ";\n")


def count_month(spark: SparkSession, settings: Settings, year_month: str) -> int:
    """Nombre de lignes d'un mois dans la table bronze."""
    df = settings.bronze_table.read(spark)
    return df.where(F.col("_year_month") == year_month).count()


def test_read_raw_csv_keeps_everything_as_text(spark: SparkSession, settings: Settings) -> None:
    """Tout reste en texte (zéros en tête compris) et la colonne vide finale disparaît."""
    path = settings.paths.raw_dir / "A202501.csv.gz"
    write_damir_file(path, ["202501;0111;23.50"])

    df = drop_unnamed_columns(read_raw_csv(spark, path, settings.source))

    assert df.columns == ["FLX_ANN_MOI", "PRS_NAT", "PRS_PAI_MNT"]
    assert {dtype for _, dtype in df.dtypes} == {"string"}
    assert df.first()["PRS_NAT"] == "0111"


def test_drop_unnamed_columns_keeps_named_ones(spark: SparkSession) -> None:
    """Seules les colonnes nommées _c<N> par Spark sont supprimées."""
    df = spark.createDataFrame([("a", "b", None)], "PRS_NAT STRING, _cible STRING, _c2 STRING")

    assert drop_unnamed_columns(df).columns == ["PRS_NAT", "_cible"]


def test_add_technical_columns(spark: SparkSession) -> None:
    """Les trois colonnes techniques sont ajoutées avec les valeurs fournies."""
    ingested_at = datetime(2026, 1, 15, 8, 30, tzinfo=UTC)
    df = spark.createDataFrame([("202501",)], "FLX_ANN_MOI STRING")

    result = add_technical_columns(df, "A202501.csv.gz", "202501", ingested_at)
    # Conversion en texte côté Spark (fuseau de session UTC) : collect() convertirait
    # l'horodatage dans le fuseau local de Python et le test dépendrait de la machine
    row = result.withColumn("_ingested_at", F.col("_ingested_at").cast("string")).first()

    assert row["_source_file"] == "A202501.csv.gz"
    assert row["_year_month"] == "202501"
    assert row["_ingested_at"] == "2026-01-15 08:30:00"


def test_ingest_month_is_idempotent(spark: SparkSession, settings: Settings) -> None:
    """Charger deux fois le même mois ne crée pas de doublons."""
    write_damir_file(settings.paths.raw_dir / "A202501.csv.gz", ["202501;1111;1.00"] * 3)

    first = ingest_month(spark, settings, 2025, 1)
    second = ingest_month(spark, settings, 2025, 1)

    assert first == second == 3
    assert count_month(spark, settings, "202501") == 3


def test_reloading_a_month_leaves_other_months_untouched(
    spark: SparkSession, settings: Settings
) -> None:
    """Charger février garde janvier ; recharger janvier remplace janvier sans toucher à février."""
    raw = settings.paths.raw_dir
    write_damir_file(raw / "A202501.csv.gz", ["202501;1111;1.00"] * 3)
    write_damir_file(raw / "A202502.csv.gz", ["202502;1111;2.00"] * 2)
    ingest_month(spark, settings, 2025, 1)
    ingest_month(spark, settings, 2025, 2)

    # Vérifié avant le rechargement : sinon un chargement de février qui effacerait
    # janvier passerait inaperçu, le rechargement de janvier le recréant
    assert count_month(spark, settings, "202501") == 3

    write_damir_file(raw / "A202501.csv.gz", ["202501;1111;1.00"])
    ingest_month(spark, settings, 2025, 1)

    assert count_month(spark, settings, "202501") == 1
    assert count_month(spark, settings, "202502") == 2


def test_missing_file_is_reported(spark: SparkSession, settings: Settings) -> None:
    """Un fichier absent produit une erreur explicite avec son chemin."""
    with pytest.raises(FileNotFoundError, match="A202503.csv.gz"):
        ingest_month(spark, settings, 2025, 3)


def test_ingest_months_loads_each_month(spark: SparkSession, settings: Settings) -> None:
    """Plusieurs mois en un appel : chacun dans sa partition, lignes comptées par mois."""
    raw = settings.paths.raw_dir
    write_damir_file(raw / "A202501.csv.gz", ["202501;1111;1.00"] * 3)
    write_damir_file(raw / "A202502.csv.gz", ["202502;1111;2.00"] * 2)

    rows_by_month = ingest_months(spark, settings, 2025, [1, 2])

    assert rows_by_month == {1: 3, 2: 2}
    assert count_month(spark, settings, "202501") == 3
    assert count_month(spark, settings, "202502") == 2


def test_ingest_months_checks_every_file_before_loading(
    spark: SparkSession, settings: Settings
) -> None:
    """Un fichier manquant est signalé avant tout chargement, même pour les mois présents."""
    write_damir_file(settings.paths.raw_dir / "A202501.csv.gz", ["202501;1111;1.00"])

    with pytest.raises(FileNotFoundError, match="A202502.csv.gz"):
        ingest_months(spark, settings, 2025, [1, 2])

    assert not (settings.paths.bronze_dir / settings.bronze.table_name).exists()


def test_invalid_month_is_rejected(spark: SparkSession, settings: Settings) -> None:
    """Un mois hors de 1..12 est refusé avant toute lecture."""
    with pytest.raises(ValueError, match="13"):
        ingest_month(spark, settings, 2025, 13)


def test_decompressed_gives_plain_csv_then_cleans_up(settings: Settings) -> None:
    """Un .gz est décompressé le temps du bloc, puis le fichier temporaire est supprimé."""
    path = settings.paths.raw_dir / "A202501.csv.gz"
    write_damir_file(path, ["202501;1111;1.00"])

    with decompressed(path) as plain:
        assert plain.suffix == ".csv"
        assert plain.read_text(encoding="utf-8").startswith(HEADER)
    assert not plain.exists()
    assert path.exists()


def test_decompressed_keeps_an_uncompressed_file_as_is(tmp_path: Path) -> None:
    """Un CSV déjà non compressé est utilisé directement, et pas supprimé."""
    plain = tmp_path / "A202501.csv"
    plain.write_text(HEADER + "\n", encoding="utf-8")

    with decompressed(plain) as used:
        assert used == plain
    assert plain.exists()


def test_decompressed_cleans_up_on_error(settings: Settings) -> None:
    """Le CSV temporaire est supprimé même si le chargement échoue."""
    path = settings.paths.raw_dir / "A202501.csv.gz"
    write_damir_file(path, ["202501;1111;1.00"])

    with pytest.raises(RuntimeError), decompressed(path) as plain:
        raise RuntimeError("échec simulé")
    assert not plain.exists()


def test_bronze_table_has_no_delta_statistics(spark: SparkSession, settings: Settings) -> None:
    """La table bronze est créée sans statistiques Delta, et le reste après un rechargement."""
    write_damir_file(settings.paths.raw_dir / "A202501.csv.gz", ["202501;1111;1.00"])

    ingest_month(spark, settings, 2025, 1)
    ingest_month(spark, settings, 2025, 1)

    assert table_properties(spark, settings.bronze_table) == BRONZE_TABLE_PROPERTIES


def test_existing_table_gets_the_property(spark: SparkSession, settings: Settings) -> None:
    """Une table bronze créée avant ce réglage reçoit la propriété au chargement suivant."""
    path = settings.paths.raw_dir / "A202501.csv.gz"
    write_damir_file(path, ["202501;1111;1.00"])
    # Table existante au format bronze, créée sans la propriété (comme avant ce réglage)
    df = drop_unnamed_columns(read_raw_csv(spark, path, settings.source))
    df = add_technical_columns(df, path.name, "202501", datetime(2026, 1, 1, tzinfo=UTC))
    overwrite_month(df, settings.bronze_table, "202501")
    assert table_properties(spark, settings.bronze_table) == {}

    ingest_month(spark, settings, 2025, 1)

    assert table_properties(spark, settings.bronze_table) == BRONZE_TABLE_PROPERTIES
