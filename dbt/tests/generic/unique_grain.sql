{#
  Test générique : la combinaison de colonnes est unique (grain de la table).
  Renvoie les combinaisons en double.
#}
{% test unique_grain(model, columns) %}
select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
