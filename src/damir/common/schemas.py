"""Schéma de la couche silver : pour chaque colonne, son nom lisible, son type et ses codes DAMIR source.

Source unique de vérité : le schéma Spark du silver et la page docs/colonnes_silver.md
sont générés à partir de SILVER_COLUMNS.

Libellés : schéma DAMIR de la documentation SNDS (Health Data Hub) et wiki SGMAP-AGD/DAMIR,
en attendant le descriptif officiel Open DAMIR.

Régénérer la documentation : uv run python -m damir.common.schemas docs/colonnes_silver.md
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql.types import (
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AMOUNT = DecimalType(18, 2)

# Colonnes des fichiers mensuels Open DAMIR, dans leur ordre d'apparition (en-tête des fichiers 2025)
DAMIR_SOURCE_COLUMNS = (
    "FLX_ANN_MOI", "ORG_CLE_REG", "AGE_BEN_SNDS", "BEN_RES_REG", "BEN_CMU_TOP", "BEN_QLT_COD",
    "BEN_SEX_COD", "DDP_SPE_COD", "ETE_CAT_SNDS", "ETE_REG_COD", "ETE_TYP_SNDS", "ETP_REG_COD",
    "ETP_CAT_SNDS", "MDT_TYP_COD", "MFT_COD", "PRS_FJH_TYP", "PRS_ACT_COG", "PRS_ACT_NBR",
    "PRS_ACT_QTE", "PRS_DEP_MNT", "PRS_PAI_MNT", "PRS_REM_BSE", "PRS_REM_MNT", "FLT_ACT_COG",
    "FLT_ACT_NBR", "FLT_ACT_QTE", "FLT_PAI_MNT", "FLT_DEP_MNT", "FLT_REM_MNT", "SOI_ANN",
    "SOI_MOI", "ASU_NAT", "ATT_NAT", "CPL_COD", "CPT_ENV_TYP", "DRG_AFF_NAT", "ETE_IND_TAA",
    "EXO_MTF", "MTM_NAT", "PRS_NAT", "PRS_PPU_SEC", "PRS_REM_TAU", "PRS_REM_TYP", "PRS_PDS_QCP",
    "EXE_INS_REG", "PSE_ACT_SNDS", "PSE_ACT_CAT", "PSE_SPE_SNDS", "PSE_STJ_SNDS", "PRE_INS_REG",
    "PSP_ACT_SNDS", "PSP_ACT_CAT", "PSP_SPE_SNDS", "PSP_STJ_SNDS", "TOP_PS5_TRG", "ETB_DCS_MCO",
)  # fmt: skip

# Colonnes techniques héritées du bronze
TECHNICAL_FIELDS = (
    StructField("_source_file", StringType()),
    StructField("_ingested_at", TimestampType()),
    StructField("_year_month", StringType()),
)


@dataclass(frozen=True)
class ColumnSpec:
    """Une colonne du silver.

    Une colonne de type date est construite à partir de codes source qui, mis bout à bout,
    donnent une année-mois « AAAAMM » (FLX_ANN_MOI, ou SOI_ANN + SOI_MOI).
    """

    name: str
    dtype: DataType
    sources: tuple[str, ...]
    label: str
    axis: str


def _col(
    name: str, dtype: DataType, source: str | tuple[str, ...], label: str, axis: str
) -> ColumnSpec:
    """Raccourci pour déclarer une colonne (un code source simple ou un tuple de codes)."""
    sources = (source,) if isinstance(source, str) else source
    return ColumnSpec(name, dtype, sources, label, axis)


# Alias courts pour garder une colonne par ligne dans le tableau ci-dessous
TEXT, INT, DATE, RATE = StringType(), IntegerType(), DateType(), DecimalType(6, 2)

SILVER_COLUMNS = (
    # Période
    _col("mois_traitement", DATE, "FLX_ANN_MOI", "Mois de traitement (mois du flux)", "Période"),
    _col("mois_soins", DATE, ("SOI_ANN", "SOI_MOI"), "Mois des soins ; nul si inconnu (0000/00)", "Période"),
    # Prestation
    _col("nature_prestation", TEXT, "PRS_NAT", "Nature de prestation", "Prestation"),
    _col("nature_assurance", TEXT, "ASU_NAT", "Nature d'assurance", "Prestation"),
    _col("nature_accident_travail", TEXT, "ATT_NAT", "Nature de l'accident du travail", "Prestation"),
    _col("type_enveloppe", TEXT, "CPT_ENV_TYP", "Type d'enveloppe", "Prestation"),
    _col("complement_acte", TEXT, "CPL_COD", "Complément d'acte (majorations hors actes CCAM)", "Prestation"),
    _col("secteur_prive_public", TEXT, "PRS_PPU_SEC", "Secteur privé / public de la prestation", "Prestation"),
    _col("motif_exoneration_ticket_moderateur", TEXT, "EXO_MTF", "Motif d'exonération du ticket modérateur", "Prestation"),
    _col("taux_remboursement", RATE, "PRS_REM_TAU", "Taux de remboursement réel du régime obligatoire (%)", "Prestation"),
    _col("modulation_ticket_moderateur", TEXT, "MTM_NAT", "Modulation du ticket modérateur", "Prestation"),
    _col("type_prise_en_charge_forfait_journalier", TEXT, "PRS_FJH_TYP", "Type de prise en charge du forfait journalier", "Prestation"),
    _col("qualificatif_parcours_soins", TEXT, "PRS_PDS_QCP", "Code qualificatif du parcours de soins", "Prestation"),
    _col("type_remboursement", TEXT, "PRS_REM_TYP", "Type de remboursement", "Prestation"),
    # Organisme
    _col("region_organisme_liquidation", TEXT, "ORG_CLE_REG", "Région de l'organisme de liquidation", "Organisme"),
    # Bénéficiaire
    _col("sexe_beneficiaire", TEXT, "BEN_SEX_COD", "Sexe du bénéficiaire", "Bénéficiaire"),
    _col("tranche_age_beneficiaire", TEXT, "AGE_BEN_SNDS", "Tranche d'âge du bénéficiaire au moment des soins", "Bénéficiaire"),
    _col("qualite_beneficiaire", TEXT, "BEN_QLT_COD", "Qualité du bénéficiaire", "Bénéficiaire"),
    _col("region_residence_beneficiaire", TEXT, "BEN_RES_REG", "Région de résidence du bénéficiaire", "Bénéficiaire"),
    _col("nature_destinataire_reglement", TEXT, "DRG_AFF_NAT", "Nature du destinataire du règlement (code affiné)", "Bénéficiaire"),
    _col("top_cmu_complementaire", TEXT, "BEN_CMU_TOP", "Top bénéficiaire de la CMU complémentaire", "Bénéficiaire"),
    # Professionnel de santé exécutant
    _col("categorie_executant", TEXT, "PSE_ACT_CAT", "Catégorie de l'exécutant", "Professionnel exécutant"),
    _col("specialite_executant", TEXT, "PSE_SPE_SNDS", "Spécialité médicale du professionnel exécutant", "Professionnel exécutant"),
    _col("nature_activite_executant", TEXT, "PSE_ACT_SNDS", "Nature d'activité du professionnel exécutant", "Professionnel exécutant"),
    _col("region_executant", TEXT, "EXE_INS_REG", "Région du professionnel exécutant", "Professionnel exécutant"),
    _col("statut_juridique_executant", TEXT, "PSE_STJ_SNDS", "Statut juridique du professionnel exécutant", "Professionnel exécutant"),
    # Établissement exécutant
    _col("region_etablissement_executant", TEXT, "ETE_REG_COD", "Région d'implantation de l'établissement exécutant", "Établissement exécutant"),
    _col("type_etablissement_executant", TEXT, "ETE_TYP_SNDS", "Type de l'établissement exécutant", "Établissement exécutant"),
    _col("categorie_etablissement_executant", TEXT, "ETE_CAT_SNDS", "Catégorie de l'établissement exécutant", "Établissement exécutant"),
    _col("discipline_prestation_etablissement", TEXT, "DDP_SPE_COD", "Discipline de prestation de l'établissement exécutant", "Établissement exécutant"),
    _col("mode_traitement_etablissement", TEXT, "MDT_TYP_COD", "Mode de traitement de l'établissement exécutant", "Établissement exécutant"),
    _col("mode_fixation_tarifs_etablissement", TEXT, "MFT_COD", "Mode de fixation des tarifs de l'établissement exécutant", "Établissement exécutant"),
    _col("indicateur_taa", TEXT, "ETE_IND_TAA", "Indicateur TAA privé / public", "Établissement exécutant"),
    _col("discipline_mco_etablissement", TEXT, "ETB_DCS_MCO", "Discipline MCO de l'établissement (libellé à confirmer)", "Établissement exécutant"),
    # Professionnel de santé prescripteur
    _col("categorie_prescripteur", TEXT, "PSP_ACT_CAT", "Catégorie du prescripteur", "Professionnel prescripteur"),
    _col("specialite_prescripteur", TEXT, "PSP_SPE_SNDS", "Spécialité médicale du professionnel prescripteur", "Professionnel prescripteur"),
    _col("nature_activite_prescripteur", TEXT, "PSP_ACT_SNDS", "Nature d'activité du professionnel prescripteur", "Professionnel prescripteur"),
    _col("region_prescripteur", TEXT, "PRE_INS_REG", "Région du professionnel prescripteur", "Professionnel prescripteur"),
    _col("statut_juridique_prescripteur", TEXT, "PSP_STJ_SNDS", "Statut juridique du professionnel prescripteur", "Professionnel prescripteur"),
    # Établissement prescripteur
    _col("region_etablissement_prescripteur", TEXT, "ETP_REG_COD", "Région d'implantation de l'établissement prescripteur", "Établissement prescripteur"),
    _col("categorie_etablissement_prescripteur", TEXT, "ETP_CAT_SNDS", "Catégorie de l'établissement prescripteur", "Établissement prescripteur"),
    # Indicateurs : acte de base (PRS_*) et prestation complète (FLT_*)
    _col("coefficient_global_acte_base", AMOUNT, "PRS_ACT_COG", "Coefficient global de l'acte de base", "Indicateurs"),
    _col("denombrement_acte_base", INT, "PRS_ACT_NBR", "Dénombrement des actes de base", "Indicateurs"),
    _col("quantite_acte_base", INT, "PRS_ACT_QTE", "Quantité de l'acte de base", "Indicateurs"),
    _col("montant_depassement", AMOUNT, "PRS_DEP_MNT", "Montant global du dépassement (€)", "Indicateurs"),
    _col("montant_depense", AMOUNT, "PRS_PAI_MNT", "Montant global de la dépense (€)", "Indicateurs"),
    _col("base_remboursement", AMOUNT, "PRS_REM_BSE", "Base de remboursement (€)", "Indicateurs"),
    _col("montant_rembourse", AMOUNT, "PRS_REM_MNT", "Montant remboursé (€)", "Indicateurs"),
    _col("coefficient_global_prestation", AMOUNT, "FLT_ACT_COG", "Coefficient global de la prestation", "Indicateurs"),
    _col("denombrement_prestation", INT, "FLT_ACT_NBR", "Dénombrement de la prestation", "Indicateurs"),
    _col("quantite_prestation", INT, "FLT_ACT_QTE", "Quantité de la prestation", "Indicateurs"),
    _col("montant_depense_prestation", AMOUNT, "FLT_PAI_MNT", "Montant de la dépense de la prestation (€)", "Indicateurs"),
    _col("montant_depassement_prestation", AMOUNT, "FLT_DEP_MNT", "Montant du dépassement de la prestation (€)", "Indicateurs"),
    _col("montant_rembourse_part_base", AMOUNT, "FLT_REM_MNT", "Montant versé / remboursé, part de base uniquement (€)", "Indicateurs"),
    # Périmètre
    _col("top_perimetre_ps5", TEXT, "TOP_PS5_TRG", "Top périmètre PS5 tous régimes", "Périmètre"),
)  # fmt: skip


def silver_schema() -> StructType:
    """Schéma Spark explicite de la table silver (colonnes métier puis techniques)."""
    business = [StructField(spec.name, spec.dtype) for spec in SILVER_COLUMNS]
    return StructType(business + list(TECHNICAL_FIELDS))


def columns_markdown() -> str:
    """Page de documentation : correspondance codes DAMIR -> colonnes silver, par axe d'analyse."""
    lines = [
        "# Colonnes de la table silver",
        "",
        "Page générée depuis `src/damir/common/schemas.py` : ne pas modifier à la main.",
        "Régénérer : `uv run python -m damir.common.schemas docs/colonnes_silver.md`",
        "",
        "Les codes de nomenclature restent en texte : ce sont des catégories, pas des nombres.",
        "",
    ]
    axes = dict.fromkeys(spec.axis for spec in SILVER_COLUMNS)
    for axis in axes:
        lines += [
            f"## {axis}",
            "",
            "| Colonne silver | Type | Code(s) DAMIR | Libellé |",
            "|---|---|---|---|",
        ]
        for spec in (s for s in SILVER_COLUMNS if s.axis == axis):
            codes = " + ".join(f"`{code}`" for code in spec.sources)
            lines.append(
                f"| `{spec.name}` | {spec.dtype.simpleString()} | {codes} | {spec.label} |"
            )
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    output = Path(sys.argv[1])
    # newline="\n" : fin de ligne LF même sous Windows, fichier identique sur tous les OS
    output.write_text(columns_markdown(), encoding="utf-8", newline="\n")
    print(f"Écrit : {output}")
