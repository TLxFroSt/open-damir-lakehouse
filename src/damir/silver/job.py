"""Couche silver : un mois du bronze devient une partition typée, plus sa quarantaine.

Chaque exécution vérifie que rien ne se perd entre bronze et silver : même nombre de
lignes et même montant total de dépense (silver + quarantaine = bronze).
"""

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from damir.common.config import Settings
from damir.common.delta import PARTITION_COLUMN, overwrite_month
from damir.common.schemas import AMOUNT
from damir.common.tables import TableRef
from damir.silver.nomenclatures import unknown_code_rows
from damir.silver.transform import REJECTION_COLUMN, rejection_reasons, to_silver_columns

logger = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """Les lignes ou les montants du silver et de la quarantaine ne correspondent pas au bronze."""


@dataclass(frozen=True)
class Totals:
    """Nombre de lignes et montant total de la dépense (PRS_PAI_MNT) d'un mois."""

    rows: int
    amount: Decimal


@dataclass(frozen=True)
class SilverMonthResult:
    """Bilan du chargement d'un mois en silver."""

    year_month: str
    bronze: Totals
    silver: Totals
    quarantine: Totals
    # Codes sans libellé et leur nombre de lignes, par code DAMIR (vide si non contrôlé)
    unknown_codes: dict[str, dict[str, int]] = field(default_factory=dict)


def check_reconciliation(bronze: Totals, silver: Totals, quarantine: Totals) -> None:
    """Lève ReconciliationError si silver + quarantaine ne redonnent pas le bronze."""
    if silver.rows + quarantine.rows != bronze.rows:
        raise ReconciliationError(
            f"Lignes : bronze {bronze.rows:,} ≠ silver {silver.rows:,} "
            f"+ quarantaine {quarantine.rows:,}"
        )
    if silver.amount + quarantine.amount != bronze.amount:
        raise ReconciliationError(
            f"Montant de la dépense : bronze {bronze.amount} ≠ silver {silver.amount} "
            f"+ quarantaine {quarantine.amount}"
        )


def _totals(df: DataFrame, amount: Column) -> Totals:
    """Nombre de lignes et somme d'un montant, en une seule lecture."""
    row = df.agg(F.count(F.lit(1)).alias("rows"), F.sum(amount).alias("amount")).first()
    return Totals(rows=row["rows"], amount=row["amount"] or Decimal("0.00"))


def _month(spark: SparkSession, table: TableRef, year_month: str) -> DataFrame:
    """Lignes d'un mois dans une table Delta."""
    return table.read(spark).where(F.col(PARTITION_COLUMN) == year_month)


def build_silver_month(
    spark: SparkSession,
    settings: Settings,
    year: int,
    month: int,
    known_codes: Mapping[str, set[str]] | None = None,
) -> SilverMonthResult:
    """Charge un mois du bronze vers le silver et la quarantaine, puis contrôle la cohérence.

    Avec `known_codes` (codes ayant un libellé, par code DAMIR), signale aussi les codes
    sans libellé du mois ; ce n'est jamais un motif de rejet.
    """
    year_month = f"{year}{month:02d}"
    started = time.perf_counter()
    bronze = _month(spark, settings.bronze_table, year_month)
    # Montant source converti comme en silver : une valeur non convertible compte pour nul
    # des deux côtés, la comparaison reste juste
    raw_amount = F.col("PRS_PAI_MNT").try_cast(AMOUNT)
    bronze_totals = _totals(bronze, raw_amount)
    if bronze_totals.rows == 0:
        raise ValueError(f"Mois {year_month} absent du bronze : lancer d'abord l'ingestion")
    logger.info("Silver %s : %s lignes bronze", year_month, f"{bronze_totals.rows:,}")

    checked = rejection_reasons(bronze)
    valid = to_silver_columns(checked.where(F.size(REJECTION_COLUMN) == 0))
    rejected = checked.where(F.size(REJECTION_COLUMN) > 0)
    # La quarantaine est réécrite même vide : un mois corrigé n'y laisse pas d'anciens rejets
    overwrite_month(valid, settings.silver_table, year_month)
    overwrite_month(rejected, settings.quarantine_table, year_month)

    # Contrôle sur ce qui a réellement été écrit sur disque
    silver_month = _month(spark, settings.silver_table, year_month)
    silver_totals = _totals(silver_month, F.col("montant_depense"))
    quarantine = _month(spark, settings.quarantine_table, year_month)
    quarantine_totals = _totals(quarantine, raw_amount)
    check_reconciliation(bronze_totals, silver_totals, quarantine_totals)

    if quarantine_totals.rows:
        reasons = (
            quarantine.select(F.explode(REJECTION_COLUMN).alias("motif"))
            .groupBy("motif")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        logger.warning(
            "Silver %s : %s lignes en quarantaine (%s)",
            year_month,
            f"{quarantine_totals.rows:,}",
            ", ".join(f"{r['motif']} : {r['count']:,}" for r in reasons),
        )
    unknown = unknown_code_rows(silver_month, known_codes) if known_codes is not None else {}
    for code_damir, codes in unknown.items():
        logger.warning(
            "Silver %s : codes %s sans libellé : %s",
            year_month,
            code_damir,
            ", ".join(
                f"{code} ({rows:,} lignes, {rows / silver_totals.rows:.2%})"
                for code, rows in codes.items()
            ),
        )
    logger.info(
        "Silver %s : %s lignes, %s en quarantaine, cohérence vérifiée (%.0f s)",
        year_month,
        f"{silver_totals.rows:,}",
        f"{quarantine_totals.rows:,}",
        time.perf_counter() - started,
    )
    return SilverMonthResult(year_month, bronze_totals, silver_totals, quarantine_totals, unknown)
