"""Référence d'une table Delta : chemin (exécution locale) ou nom Unity Catalog (Databricks).

Module sans dépendance à PySpark à l'import : la configuration peut s'en servir sans charger Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameWriter, SparkSession


@dataclass(frozen=True)
class TableRef:
    """Table Delta : chemin (exécution locale) ou nom Unity Catalog (Databricks)."""

    location: str
    is_path: bool

    @property
    def sql_name(self) -> str:
        """Nom utilisable en SQL : delta.`chemin` ou catalogue.schéma.table."""
        return f"delta.`{self.location}`" if self.is_path else self.location

    def read(self, spark: SparkSession) -> DataFrame:
        """Lecture de toute la table."""
        if self.is_path:
            return spark.read.format("delta").load(self.location)
        return spark.read.table(self.location)

    def save(self, writer: DataFrameWriter) -> None:
        """Termine une écriture préparée : par chemin, ou comme table du catalogue."""
        if self.is_path:
            writer.save(self.location)
        else:
            writer.saveAsTable(self.location)
