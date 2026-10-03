{#
  Libellé d'un code DAMIR, en sous-requête scalaire : la ligne est toujours conservée,
  et un code sans libellé donne null (signalé par un test en avertissement).

  Exemple : {{ libelle('PRS_NAT', 'nature_prestation') }} as libelle_nature_prestation
#}
{% macro libelle(code_damir, code_column) -%}
    (
        select n.libelle
        from {{ ref('stg_nomenclatures') }} as n
        where n.code_damir = '{{ code_damir }}'
            and n.code = {{ code_column }}
    )
{%- endmacro %}
