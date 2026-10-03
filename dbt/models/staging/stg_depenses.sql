-- Dépenses du silver, limitées aux colonnes utilisées par la couche gold
select
    mois_traitement,
    nature_prestation,
    region_residence_beneficiaire,
    montant_depense,
    base_remboursement,
    montant_rembourse,
    montant_depassement
from {{ source('silver', 'open_damir') }}
