# Nomenclatures Open DAMIR

`nomenclatures.csv` donne le libellé de chaque code des 39 colonnes de codes DAMIR
(`code_damir;code;libelle;source`). Il est **généré** : ne pas le modifier à la main.

```
uv run python -m damir.reference.nomenclatures
```

## Pourquoi ce fichier

Le descriptif officiel Open DAMIR (« Descriptif des variables de la série Open Damir base
complète ») n'est pas téléchargeable : le lien publié par l'Assurance Maladie et data.gouv.fr
renvoie une erreur 404. Les libellés sont donc reconstitués à partir de sources ouvertes,
versionnées à un commit précis pour que la reconstruction soit reproductible.

## Sources

| Source | Contenu | Licence |
|---|---|---|
| [schema-snds](https://gitlab.com/healthdatahub/applications-du-hdh/schema-snds) (Health Data Hub), commit `7bfdf37` | Nomenclatures SNDS à jour (`IR_NAT_V`, `IR_REG_V`, `IR_CET_D`...) | MPL 2.0 |
| Lexique Open DAMIR 2015 (CNAM), copie du dépôt [SGMAP-AGD/DAMIR](https://github.com/SGMAP-AGD/DAMIR), commit `61db19a` | Nomenclatures propres à Open DAMIR (regroupements) | Données ouvertes CNAM |
| `complements_nomenclatures.csv` | 10 codes absents des deux sources, chacun avec sa justification | Ce projet |

## Règle de priorité

Choisie colonne par colonne après comparaison des libellés sur janvier 2025
(détail dans `src/damir/reference/nomenclatures.py`) :

- **codes bruts du SNDS** (nature de prestation, exonération, régions...) : SNDS d'abord, plus
  récent (par exemple `CPT_ENV_TYP = 7` : « C2S, AME ou ACS », contre « CMU complémentaire » en 2015) ;
- **regroupements propres à Open DAMIR** (tranches d'âge, spécialités et activités regroupées) :
  lexique d'abord. La table SNDS des âges ne doit pas servir : `AGE_BEN_SNDS = 70` signifie
  « 70 - 79 ans » dans Open DAMIR, « 70 - 74 ans » dans le SNDS.

Les compléments ne couvrent que des codes absents de toutes les sources : la construction
échoue si un complément doublonne une source.

## Couverture (janvier 2025)

- 36 colonnes sur 39 entièrement décodées ; 99,95 % des valeurs de codes ont un libellé.
- Sans libellé, faute de source : `PRS_PDS_QCP = 34` (1,8 % des lignes),
  `PRS_REM_TYP = 11, 12, 13` (0,3 %), `DDP_SPE_COD = 46` (948 lignes).
- Libellés déduits, à confirmer : région `5` (regroupement Corse et outre-mer).
