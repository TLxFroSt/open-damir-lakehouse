-- Dépenses mensuelles par nature de prestation : où va l'argent ?
-- Agrégation d'abord, libellés ensuite (quelques centaines de lignes à décoder, pas 35 millions)
with par_prestation as (
    select
        mois_traitement,
        nature_prestation,
        count(*) as nombre_lignes,
        sum(montant_depense) as montant_depense,
        sum(base_remboursement) as base_remboursement,
        sum(montant_rembourse) as montant_rembourse,
        sum(montant_depassement) as montant_depassement
    from {{ ref('stg_depenses') }}
    group by mois_traitement, nature_prestation
)

select
    mois_traitement,
    nature_prestation,
    {{ libelle('PRS_NAT', 'par_prestation.nature_prestation') }} as libelle_nature_prestation,
    nombre_lignes,
    montant_depense,
    base_remboursement,
    montant_rembourse,
    montant_depassement,
    montant_depense - montant_rembourse as montant_non_rembourse,
    {{ ratio('montant_rembourse', 'montant_depense') }} as taux_remboursement_effectif,
    {{ ratio('montant_depassement', 'montant_depense') }} as part_depassement
from par_prestation
