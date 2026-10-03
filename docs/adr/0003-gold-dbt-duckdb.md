# ADR 0003 — Couche gold : dbt avec DuckDB en local

- Statut : accepté
- Date : 2026-10-03

## Contexte

La couche gold produit les agrégats métier avec dbt. La cible finale est Databricks
(`dbt-databricks`) ; en local, il faut un moteur qui lise les tables Delta écrites par Spark.
Deux options :

- **dbt-spark** (mode session) : même dialecte SQL que Databricks, mais chaque exécution démarre
  une session Spark (~10 s), avec la configuration Delta à reproduire hors du code du projet ;
- **dbt-duckdb** : DuckDB lit les tables Delta directement (`delta_scan`, extension `delta`).

Mesure sur le silver réel (71,4 M de lignes, janvier et février 2025) : DuckDB agrège la
table entière en 0,4 s, avec les types attendus (`DECIMAL(18,2)`, `DATE`).

## Décisions

1. **dbt-duckdb en local.** Les sources pointent vers les tables Delta du silver ; la couche gold
   est stockée dans un fichier DuckDB (`data/gold/damir.duckdb`, non versionné).
2. **SQL portable.** Pas de fonction propre à DuckDB dans les modèles (agrégats, `nullif`,
   `round`, `cast`, fenêtres, `is distinct from`) : le passage à Databricks ne doit changer que
   le profil et la définition des sources.
3. **Une seule configuration.** `python -m damir.gold` lit `config.yaml` et passe les chemins à
   dbt (variables pour les sources, variable d'environnement pour le fichier DuckDB).
4. **Tests sans paquet externe.** Deux tests génériques écrits dans le projet : `unique_grain`
   (grain d'une table) et `matches_silver_totals` (total mensuel du gold = total du silver).
   Pas de `dbt deps`, donc pas d'accès réseau supplémentaire en CI.
5. **Libellés après agrégation.** Les tables agrègent d'abord puis décodent (macro `nomenclature`) :
   quelques centaines de lignes à décoder au lieu de 35 millions. Un code sans libellé donne un
   libellé nul et un avertissement, jamais une ligne perdue.

## Conséquences

- La couche gold se construit en 7 s sur les données réelles, tests compris.
- La CI teste le SQL de bout en bout : un silver synthétique est écrit par Spark puis `dbt build`
  est exécuté dessus.
- Déploiement Databricks réalisé ensuite (ADR 0004) : la portabilité supposée ici n'était pas
  complète, les sous-requêtes scalaires corrélées ont dû être remplacées par des `LEFT JOIN`.
