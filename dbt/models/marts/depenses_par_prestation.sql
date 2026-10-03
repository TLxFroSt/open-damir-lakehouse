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
    par_prestation.mois_traitement,
    par_prestation.nature_prestation,
    lib.libelle as libelle_nature_prestation,
    par_prestation.nombre_lignes,
    par_prestation.montant_depense,
    par_prestation.base_remboursement,
    par_prestation.montant_rembourse,
    par_prestation.montant_depassement,
    par_prestation.montant_depense - par_prestation.montant_rembourse as montant_non_rembourse,
    {{ ratio('par_prestation.montant_rembourse', 'par_prestation.montant_depense') }}
        as taux_remboursement_effectif,
    {{ ratio('par_prestation.montant_depassement', 'par_prestation.montant_depense') }}
        as part_depassement
from par_prestation
left join {{ nomenclature('PRS_NAT') }} as lib
    on lib.code = par_prestation.nature_prestation
