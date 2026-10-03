{{ config(severity='warn') }}
{#
  Codes du silver sans libellé dans la table des nomenclatures, sur tous les mois.
  Avertissement seulement : un code inconnu n'est jamais un motif de rejet (ADR 0001).
  Renvoie (code DAMIR, code, nombre de lignes) pour chaque code sans libellé.

  Liste (colonne silver, code DAMIR) synchronisée avec src/damir/common/schemas.py
  (vérifié par tests/test_dbt_project.py).
#}
{% set colonnes_de_codes = [
    ('nature_prestation', 'PRS_NAT'),
    ('nature_assurance', 'ASU_NAT'),
    ('nature_accident_travail', 'ATT_NAT'),
    ('type_enveloppe', 'CPT_ENV_TYP'),
    ('complement_acte', 'CPL_COD'),
    ('secteur_prive_public', 'PRS_PPU_SEC'),
    ('motif_exoneration_ticket_moderateur', 'EXO_MTF'),
    ('modulation_ticket_moderateur', 'MTM_NAT'),
    ('type_prise_en_charge_forfait_journalier', 'PRS_FJH_TYP'),
    ('qualificatif_parcours_soins', 'PRS_PDS_QCP'),
    ('type_remboursement', 'PRS_REM_TYP'),
    ('region_organisme_liquidation', 'ORG_CLE_REG'),
    ('sexe_beneficiaire', 'BEN_SEX_COD'),
    ('tranche_age_beneficiaire', 'AGE_BEN_SNDS'),
    ('qualite_beneficiaire', 'BEN_QLT_COD'),
    ('region_residence_beneficiaire', 'BEN_RES_REG'),
    ('nature_destinataire_reglement', 'DRG_AFF_NAT'),
    ('top_cmu_complementaire', 'BEN_CMU_TOP'),
    ('categorie_executant', 'PSE_ACT_CAT'),
    ('specialite_executant', 'PSE_SPE_SNDS'),
    ('nature_activite_executant', 'PSE_ACT_SNDS'),
    ('region_executant', 'EXE_INS_REG'),
    ('statut_juridique_executant', 'PSE_STJ_SNDS'),
    ('region_etablissement_executant', 'ETE_REG_COD'),
    ('type_etablissement_executant', 'ETE_TYP_SNDS'),
    ('categorie_etablissement_executant', 'ETE_CAT_SNDS'),
    ('discipline_prestation_etablissement', 'DDP_SPE_COD'),
    ('mode_traitement_etablissement', 'MDT_TYP_COD'),
    ('mode_fixation_tarifs_etablissement', 'MFT_COD'),
    ('indicateur_taa', 'ETE_IND_TAA'),
    ('discipline_mco_etablissement', 'ETB_DCS_MCO'),
    ('categorie_prescripteur', 'PSP_ACT_CAT'),
    ('specialite_prescripteur', 'PSP_SPE_SNDS'),
    ('nature_activite_prescripteur', 'PSP_ACT_SNDS'),
    ('region_prescripteur', 'PRE_INS_REG'),
    ('statut_juridique_prescripteur', 'PSP_STJ_SNDS'),
    ('region_etablissement_prescripteur', 'ETP_REG_COD'),
    ('categorie_etablissement_prescripteur', 'ETP_CAT_SNDS'),
    ('top_perimetre_ps5', 'TOP_PS5_TRG'),
] %}

with codes as (
    {% for colonne, code_damir in colonnes_de_codes %}
    select '{{ code_damir }}' as code_damir, {{ colonne }} as code, count(*) as lignes
    from {{ source('silver', 'open_damir') }}
    where {{ colonne }} is not null
    group by {{ colonne }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select codes.code_damir, codes.code, codes.lignes
from codes
left join {{ ref('stg_nomenclatures') }} as n
    on n.code_damir = codes.code_damir
    and n.code = codes.code
where n.code is null
