"""Fixtures partagées par les tests."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from damir.common.config import (
    BronzeConfig,
    PathsConfig,
    Settings,
    SilverConfig,
    SourceConfig,
    SparkConfig,
    StorageConfig,
)
from damir.common.schemas import DAMIR_SOURCE_COLUMNS, TECHNICAL_FIELDS
from damir.common.spark import build_spark_session

# Une vraie ligne de janvier 2025, telle qu'elle apparaît dans le fichier (sans le « ; » final)
VALID_BRONZE_LINE = (
    "202501;32;70;99;0;1;1;121;9999;99;99;99;9999;9;99;0;23.21;22;22;0;696.3;696.3;696.3;"
    "23.21;22;22;696.3;0;696.3;2025;01;10;0;0;1;35;9;42;0;3134;2;100;0;31;32;27;8;0;1;32;"
    "27;8;0;1;1;Z"
)
VALID_BRONZE_ROW = dict(zip(DAMIR_SOURCE_COLUMNS, VALID_BRONZE_LINE.split(";"), strict=True))


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """Session Spark unique pour toute la suite, réglée pour de petits volumes."""
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_spark_session(
        app_name="damir-tests",
        master="local[2]",
        extra_conf={
            # Une seule partition de shuffle : les jeux de test sont minuscules
            "spark.sql.shuffle.partitions": "1",
            "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": warehouse.as_posix(),
        },
    )
    yield session
    session.stop()


def bronze_rows(spark: SparkSession) -> Callable[..., DataFrame]:
    """Fabrique un DataFrame au format bronze : chaque ligne part de VALID_BRONZE_ROW.

    Exemple : bronze_rows(spark)({"PRS_PAI_MNT": "abc"}, {}) -> une ligne modifiée, une valide.
    """
    schema = StructType(
        [StructField(code, StringType()) for code in DAMIR_SOURCE_COLUMNS] + list(TECHNICAL_FIELDS)
    )
    technical = ("A202501.csv.gz", datetime(2026, 1, 15, tzinfo=UTC), "202501")

    def make(*overrides: dict[str, str | None]) -> DataFrame:
        rows = [
            tuple({**VALID_BRONZE_ROW, **override}[c] for c in DAMIR_SOURCE_COLUMNS) + technical
            for override in overrides
        ]
        return spark.createDataFrame(rows, schema)

    return make


@pytest.fixture
def make_bronze(spark: SparkSession) -> Callable[..., DataFrame]:
    """Fixture de bronze_rows pour les tests à portée fonction."""
    return bronze_rows(spark)


def make_settings(base_dir: Path) -> Settings:
    """Configuration de test pointant vers des dossiers sous `base_dir`."""
    return Settings(
        paths=PathsConfig(
            raw_dir=base_dir / "raw",
            bronze_dir=base_dir / "bronze",
            silver_dir=base_dir / "silver",
            nomenclatures_file=base_dir / "nomenclatures.csv",
            gold_db=base_dir / "gold" / "damir.duckdb",
        ),
        source=SourceConfig(
            file_name_template="A{year}{month:02d}.csv.gz",
            csv_separator=";",
            csv_encoding="UTF-8",
        ),
        spark=SparkConfig(master="local[2]", app_name="test", driver_memory="1g"),
        bronze=BronzeConfig(table_name="open_damir"),
        silver=SilverConfig(
            table_name="open_damir",
            quarantine_table_name="open_damir_quarantine",
            nomenclatures_table_name="nomenclatures",
        ),
        storage=StorageConfig(mode="path", catalog="workspace"),
    )


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Configuration pointant vers des dossiers temporaires."""
    return make_settings(tmp_path)
