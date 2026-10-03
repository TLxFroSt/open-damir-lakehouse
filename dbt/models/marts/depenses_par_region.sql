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
    par_region.mois_traitement,
    par_region.region_residence_beneficiaire,
    lib.libelle as libelle_region_residence_beneficiaire,
    par_region.nombre_lignes,
    par_region.montant_depense,
    par_region.base_remboursement,
    par_region.montant_rembourse,
    par_region.montant_depassement,
    par_region.montant_depense - par_region.montant_rembourse as montant_non_rembourse,
    {{ ratio('par_region.montant_rembourse', 'par_region.montant_depense') }}
        as taux_remboursement_effectif,
    {{ ratio(
        'par_region.montant_depense',
        'sum(par_region.montant_depense) over (partition by par_region.mois_traitement)'
    ) }} as part_depense_nationale
from par_region
left join {{ nomenclature('BEN_RES_REG') }} as lib
    on lib.code = par_region.region_residence_beneficiaire
