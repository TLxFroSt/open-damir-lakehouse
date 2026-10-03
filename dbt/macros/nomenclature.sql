{#
  Libellés d'une colonne de codes DAMIR, à joindre avec un LEFT JOIN :

    left join {{ nomenclature('PRS_NAT') }} as lib
        on lib.code = par_prestation.nature_prestation

  LEFT JOIN plutôt qu'une sous-requête scalaire corrélée : portable entre DuckDB et Databricks
  SQL (qui limite les sous-requêtes corrélées, notamment à côté des fonctions de fenêtre).
  Aucune ligne perdue (code sans libellé -> null, test en avertissement) ni dupliquée : un code
  a au plus un libellé (test unique_grain de stg_nomenclatures).
#}
{% macro nomenclature(code_damir) -%}
    (
        select code, libelle
        from {{ ref('stg_nomenclatures') }}
        where code_damir = '{{ code_damir }}'
    )
{%- endmacro %}
