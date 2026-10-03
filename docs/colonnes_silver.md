# Colonnes de la table silver

Page générée depuis `src/damir/common/schemas.py` : ne pas modifier à la main.
Régénérer : `uv run python -m damir.common.schemas docs/colonnes_silver.md`

Les codes de nomenclature restent en texte : ce sont des catégories, pas des nombres.

## Période

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `mois_traitement` | date | `FLX_ANN_MOI` | Mois de traitement (mois du flux) |
| `mois_soins` | date | `SOI_ANN` + `SOI_MOI` | Mois des soins ; nul si inconnu (0000/00 ou 0001/01) |

## Prestation

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `nature_prestation` | string | `PRS_NAT` | Nature de prestation |
| `nature_assurance` | string | `ASU_NAT` | Nature d'assurance |
| `nature_accident_travail` | string | `ATT_NAT` | Nature de l'accident du travail |
| `type_enveloppe` | string | `CPT_ENV_TYP` | Type d'enveloppe |
| `complement_acte` | string | `CPL_COD` | Complément d'acte (majorations hors actes CCAM) |
| `secteur_prive_public` | string | `PRS_PPU_SEC` | Secteur privé / public de la prestation |
| `motif_exoneration_ticket_moderateur` | string | `EXO_MTF` | Motif d'exonération du ticket modérateur |
| `taux_remboursement` | decimal(6,2) | `PRS_REM_TAU` | Taux de remboursement réel du régime obligatoire (%) |
| `modulation_ticket_moderateur` | string | `MTM_NAT` | Modulation du ticket modérateur |
| `type_prise_en_charge_forfait_journalier` | string | `PRS_FJH_TYP` | Type de prise en charge du forfait journalier |
| `qualificatif_parcours_soins` | string | `PRS_PDS_QCP` | Code qualificatif du parcours de soins |
| `type_remboursement` | string | `PRS_REM_TYP` | Type de remboursement |

## Organisme

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `region_organisme_liquidation` | string | `ORG_CLE_REG` | Région de l'organisme de liquidation |

## Bénéficiaire

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `sexe_beneficiaire` | string | `BEN_SEX_COD` | Sexe du bénéficiaire |
| `tranche_age_beneficiaire` | string | `AGE_BEN_SNDS` | Tranche d'âge du bénéficiaire au moment des soins |
| `qualite_beneficiaire` | string | `BEN_QLT_COD` | Qualité du bénéficiaire |
| `region_residence_beneficiaire` | string | `BEN_RES_REG` | Région de résidence du bénéficiaire |
| `nature_destinataire_reglement` | string | `DRG_AFF_NAT` | Nature du destinataire du règlement (code affiné) |
| `top_cmu_complementaire` | string | `BEN_CMU_TOP` | Top bénéficiaire de la CMU complémentaire |

## Professionnel exécutant

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `categorie_executant` | string | `PSE_ACT_CAT` | Catégorie de l'exécutant |
| `specialite_executant` | string | `PSE_SPE_SNDS` | Spécialité médicale du professionnel exécutant |
| `nature_activite_executant` | string | `PSE_ACT_SNDS` | Nature d'activité du professionnel exécutant |
| `region_executant` | string | `EXE_INS_REG` | Région du professionnel exécutant |
| `statut_juridique_executant` | string | `PSE_STJ_SNDS` | Statut juridique du professionnel exécutant |

## Établissement exécutant

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `region_etablissement_executant` | string | `ETE_REG_COD` | Région d'implantation de l'établissement exécutant |
| `type_etablissement_executant` | string | `ETE_TYP_SNDS` | Type de l'établissement exécutant |
| `categorie_etablissement_executant` | string | `ETE_CAT_SNDS` | Catégorie de l'établissement exécutant |
| `discipline_prestation_etablissement` | string | `DDP_SPE_COD` | Discipline de prestation de l'établissement exécutant |
| `mode_traitement_etablissement` | string | `MDT_TYP_COD` | Mode de traitement de l'établissement exécutant |
| `mode_fixation_tarifs_etablissement` | string | `MFT_COD` | Mode de fixation des tarifs de l'établissement exécutant |
| `indicateur_taa` | string | `ETE_IND_TAA` | Indicateur TAA privé / public |
| `discipline_mco_etablissement` | string | `ETB_DCS_MCO` | Discipline MCO de l'établissement (libellé à confirmer) |

## Professionnel prescripteur

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `categorie_prescripteur` | string | `PSP_ACT_CAT` | Catégorie du prescripteur |
| `specialite_prescripteur` | string | `PSP_SPE_SNDS` | Spécialité médicale du professionnel prescripteur |
| `nature_activite_prescripteur` | string | `PSP_ACT_SNDS` | Nature d'activité du professionnel prescripteur |
| `region_prescripteur` | string | `PRE_INS_REG` | Région du professionnel prescripteur |
| `statut_juridique_prescripteur` | string | `PSP_STJ_SNDS` | Statut juridique du professionnel prescripteur |

## Établissement prescripteur

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `region_etablissement_prescripteur` | string | `ETP_REG_COD` | Région d'implantation de l'établissement prescripteur |
| `categorie_etablissement_prescripteur` | string | `ETP_CAT_SNDS` | Catégorie de l'établissement prescripteur |

## Indicateurs

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `coefficient_global_acte_base` | decimal(18,2) | `PRS_ACT_COG` | Coefficient global de l'acte de base |
| `denombrement_acte_base` | int | `PRS_ACT_NBR` | Dénombrement des actes de base |
| `quantite_acte_base` | int | `PRS_ACT_QTE` | Quantité de l'acte de base |
| `montant_depassement` | decimal(18,2) | `PRS_DEP_MNT` | Montant global du dépassement (€) |
| `montant_depense` | decimal(18,2) | `PRS_PAI_MNT` | Montant global de la dépense (€) |
| `base_remboursement` | decimal(18,2) | `PRS_REM_BSE` | Base de remboursement (€) |
| `montant_rembourse` | decimal(18,2) | `PRS_REM_MNT` | Montant remboursé (€) |
| `coefficient_global_prestation` | decimal(18,2) | `FLT_ACT_COG` | Coefficient global de la prestation |
| `denombrement_prestation` | int | `FLT_ACT_NBR` | Dénombrement de la prestation |
| `quantite_prestation` | int | `FLT_ACT_QTE` | Quantité de la prestation |
| `montant_depense_prestation` | decimal(18,2) | `FLT_PAI_MNT` | Montant de la dépense de la prestation (€) |
| `montant_depassement_prestation` | decimal(18,2) | `FLT_DEP_MNT` | Montant du dépassement de la prestation (€) |
| `montant_rembourse_part_base` | decimal(18,2) | `FLT_REM_MNT` | Montant versé / remboursé, part de base uniquement (€) |

## Périmètre

| Colonne silver | Type | Code(s) DAMIR | Libellé |
|---|---|---|---|
| `top_perimetre_ps5` | string | `TOP_PS5_TRG` | Top périmètre PS5 tous régimes |
