{#
  Test générique : pour chaque mois de traitement, le total de la colonne dans la table
  gold est exactement celui du silver (rien de perdu ni de compté deux fois).
  Renvoie les mois en écart.
#}
{% test matches_silver_totals(model, column_name) %}
with gold as (
    select mois_traitement, sum({{ column_name }}) as total
    from {{ model }}
    group by mois_traitement
),

silver as (
    select mois_traitement, sum({{ column_name }}) as total
    from {{ ref('stg_depenses') }}
    group by mois_traitement
)

select
    coalesce(gold.mois_traitement, silver.mois_traitement) as mois_traitement,
    gold.total as total_gold,
    silver.total as total_silver
from gold
full outer join silver on gold.mois_traitement = silver.mois_traitement
where gold.total is distinct from silver.total
{% endtest %}
