"""Transformations bronze -> silver : fonctions pures DataFrame -> DataFrame, sans I/O."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

from damir.common.schemas import SILVER_COLUMNS, TECHNICAL_FIELDS, ColumnSpec

REJECTION_COLUMN = "_motifs_rejet"

# Mois des soins non renseigné dans DAMIR : SOI_ANN = 0000 et SOI_MOI = 00
UNKNOWN_YEAR_MONTH = "000000"

# Colonnes numériques (montants, coefficients, dénombrements, taux) : conversion contrôlée
_NUMERIC_SPECS = [
    spec for spec in SILVER_COLUMNS if not isinstance(spec.dtype, StringType | DateType)
]


def parse_year_month(value: Column) -> Column:
    """Texte « AAAAMM » -> date du 1er du mois ; nul si ce n'est pas une année-mois valide."""
    year = F.substring(value, 1, 4).try_cast("int")
    month = F.substring(value, 5, 2).try_cast("int")
    is_valid = value.rlike(r"^[0-9]{6}$") & year.between(1900, 2100) & month.between(1, 12)
    # make_date n'est évalué que sur les lignes valides : en mode ANSI (défaut de Spark 4),
    # une date impossible lèverait une erreur au lieu de renvoyer nul
    return F.when(is_valid, F.make_date(year, month, F.lit(1)))


def _year_month_text(spec: ColumnSpec) -> Column:
    """Codes source d'une colonne date mis bout à bout : « AAAAMM »."""
    return F.concat(*[F.col(code) for code in spec.sources])


def _typed(spec: ColumnSpec) -> Column:
    """Valeur silver d'une colonne : date reconstituée, ou conversion du texte DAMIR.

    try_cast renvoie nul au lieu d'échouer sur une valeur non convertible ; ces lignes
    sont repérées par rejection_reasons() avant d'arriver ici.
    """
    if isinstance(spec.dtype, DateType):
        return parse_year_month(_year_month_text(spec))
    return F.col(spec.sources[0]).try_cast(spec.dtype)


def to_silver_columns(bronze: DataFrame) -> DataFrame:
    """Renomme et type les colonnes DAMIR selon SILVER_COLUMNS, colonnes techniques conservées."""
    business = [_typed(spec).alias(spec.name) for spec in SILVER_COLUMNS]
    technical = [F.col(field.name) for field in TECHNICAL_FIELDS]
    return bronze.select(*business, *technical)


def rejection_reasons(bronze: DataFrame) -> DataFrame:
    """Ajoute la liste des motifs de rejet de chaque ligne (liste vide : ligne valide).

    Motifs :
    - non_numerique:<CODE> : valeur présente mais non convertible dans son type numérique
    - mois_traitement_incoherent : FLX_ANN_MOI différent du mois de la partition
    - mois_soins_invalide : mois des soins renseigné mais impossible (mois 13...)
    - soins_apres_traitement : soins postérieurs au mois de traitement
    Les codes de nomenclature inconnus ne sont pas des motifs de rejet : la dépense est réelle.
    """
    care = next(s for s in SILVER_COLUMNS if s.name == "mois_soins")
    processing = next(s for s in SILVER_COLUMNS if s.name == "mois_traitement")
    care_text, care_date = _year_month_text(care), _typed(care)

    checks = [
        F.when(
            F.col(spec.sources[0]).isNotNull() & _typed(spec).isNull(),
            F.lit(f"non_numerique:{spec.sources[0]}"),
        )
        for spec in _NUMERIC_SPECS
    ]
    checks += [
        F.when(
            F.col("FLX_ANN_MOI") != F.col("_year_month"),
            F.lit("mois_traitement_incoherent"),
        ),
        F.when(
            (care_text != UNKNOWN_YEAR_MONTH) & care_date.isNull(),
            F.lit("mois_soins_invalide"),
        ),
        F.when(care_date > _typed(processing), F.lit("soins_apres_traitement")),
    ]
    reasons = F.filter(F.array(*checks), lambda reason: reason.isNotNull())
    return bronze.withColumn(REJECTION_COLUMN, reasons)
