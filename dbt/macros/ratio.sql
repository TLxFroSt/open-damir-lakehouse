{#
  Rapport numérateur / dénominateur arrondi à 4 décimales, null si le dénominateur est nul.
  Peut sortir de [0, 1] : les régularisations (montants négatifs) sont conservées.
#}
{% macro ratio(numerator, denominator) -%}
    cast(round({{ numerator }} / nullif({{ denominator }}, 0), 4) as decimal(9, 4))
{%- endmacro %}
