-- Dépenses mensuelles par région de résidence du bénéficiaire
with par_region as (
    select
        mois_traitement,
        region_residence_beneficiaire,
        count(*) as nombre_lignes,
        sum(montant_depense) as montant_depense,
        sum(base_remboursement) as base_remboursement,
        sum(montant_rembourse) as montant_rembourse,
        sum(montant_depassement) as montant_depassement
    from {{ ref('stg_depenses') }}
    group by mois_traitement, region_residence_beneficiaire
)

select
    mois_traitement,
    region_residence_beneficiaire,
    {{ libelle('BEN_RES_REG', 'par_region.region_residence_beneficiaire') }}
        as libelle_region_residence_beneficiaire,
    nombre_lignes,
    montant_depense,
    base_remboursement,
    montant_rembourse,
    montant_depassement,
    montant_depense - montant_rembourse as montant_non_rembourse,
    {{ ratio('montant_rembourse', 'montant_depense') }} as taux_remboursement_effectif,
    {{ ratio('montant_depense', 'sum(montant_depense) over (partition by mois_traitement)') }}
        as part_depense_nationale
from par_region
