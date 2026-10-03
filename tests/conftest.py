"""Fixtures partagées par les tests."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from damir.common.spark import build_spark_session


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
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
