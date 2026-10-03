-- Libellés des codes DAMIR : une ligne par (code DAMIR, code)
select
    code_damir,
    code,
    libelle
from {{ source('silver', 'nomenclatures') }}
