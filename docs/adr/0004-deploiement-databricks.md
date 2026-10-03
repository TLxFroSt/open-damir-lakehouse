# ADR 0004 — Déploiement sur Databricks (Free Edition, serverless)

- Statut : accepté
- Date : 2026-10-03

## Contexte

Le pipeline tourne en local (PySpark, Delta par chemins, dbt sur DuckDB). La cible est Databricks
Free Edition : serverless uniquement (Spark Connect), Unity Catalog, un entrepôt SQL 2X-Small,
5 tâches de job simultanées au plus. L'objectif est que le même code tourne aux deux endroits et
donne les mêmes résultats.

## Décisions

1. **Un même code, deux modes de stockage.** `storage.mode` vaut `path` (dossiers Delta, local)
   ou `catalog` (tables Unity Catalog `workspace.<couche>.<table>`). Une `TableRef` porte la
   différence (lecture, `save` ou `saveAsTable`, nom SQL) ; le reste du code l'ignore.
2. **Session fournie par la plateforme.** En mode `catalog`, le code prend la session existante
   et ne règle que le fuseau (UTC) ; jars, mémoire et journalisation relèvent de Databricks.
   L'API Python `DeltaTable` (absente via Spark Connect) est remplacée par `DESCRIBE HISTORY`.
3. **Wheel sans PySpark.** Le paquet ne déclare que `pyyaml` ; PySpark, Delta et dbt-duckdb sont
   dans un groupe `local`. Installer `pyspark` dans l'environnement serverless entrerait en
   conflit avec Databricks Connect.
4. **Databricks Asset Bundle.** `databricks.yml` décrit l'infrastructure (schémas `bronze`,
   `silver`, `gold`, volume des fichiers bruts) et un job en trois tâches serverless : ingestion
   et silver en tâches Python wheel, puis `dbt build` sur l'entrepôt SQL (dbt-databricks).
   Pas de `mode: development`, qui préfixerait les noms de schémas.
5. **SQL gold portable par LEFT JOIN.** Les libellés étaient d'abord ajoutés par une sous-requête
   scalaire corrélée, acceptée par DuckDB et par Spark 4.2 en local mais refusée par Databricks
   SQL (agrégation exigée, interdite à côté d'une fonction de fenêtre, puis erreur « trop de
   lignes »). La jointure gauche sur la table des nomenclatures est portable ; les tests de
   grain et de totaux garantissent qu'elle ne perd ni ne duplique aucune ligne.

## Vérification

Job complet sur janvier et février 2025 : bronze, silver, quarantaine, nomenclatures et gold
**identiques au local, au centime près** (71 350 495 lignes ; 31 647 292 132,77 € de dépense).

Durées sur serverless : bronze ~11 min par mois (lecture gzip sur un seul cœur, comme en local),
silver ~1,5 min par mois (contre ~7 min en local), gold ~1,5 min sur l'entrepôt SQL.

## Conséquences

- Une modification se teste d'abord en local (98 tests, CI), puis se déploie par
  `databricks bundle deploy` ; une tâche en échec se relance seule (`repair-run`) sans refaire
  l'ingestion.
- Leçon retenue : la compatibilité SQL se vérifie sur la plateforme cible elle-même ; un moteur
  local « de la même famille » ne suffit pas.
