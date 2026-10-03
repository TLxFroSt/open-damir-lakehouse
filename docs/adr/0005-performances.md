# ADR 0005 — Performances du pipeline

- Statut : accepté
- Date : 2026-10-03

## Contexte

Avant optimisation, un mois prenait ~10 min en bronze et ~7 min en silver en local, et le job
Databricks complet ~28 min pour deux mois. Plutôt que d'optimiser au jugé, chaque étape a été
mesurée (durée par étape Spark, interface REST de Spark) sur janvier 2025, puis chaque
changement mesuré seul.

Mesures de départ, janvier 2025 en local :

- bronze 617 s : **417 s dans une seule tâche** (lecture du `.gz`, non découpable, sur un cœur,
  plus l'écriture du shuffle), 181 s pour le shuffle et l'écriture Parquet ;
- silver 330 s : écriture du silver 210 s, **écriture de la quarantaine 76 s** (relecture
  complète du mois pour écrire zéro ligne), contrôle des codes sans libellé 27 s.

## Décisions

1. **Décompresser avant Spark.** Python décompresse le `.gz` (~15-30 s) dans un CSV temporaire
   sous le dossier des fichiers bruts (visible des exécuteurs, en local comme dans un volume
   Databricks), supprimé ensuite même en cas d'erreur. Le CSV est découpable : tous les cœurs le
   lisent, sans `repartition` ni shuffle. Coût : ~6 Go de disque temporaire par mois.
2. **Pas de statistiques Delta sur le bronze** (`delta.dataSkippingNumIndexedCols = 0`) : il
   n'est lu qu'en entier, mois par mois. Mesuré : écriture 232 s avec, 112 s sans.
3. **Quarantaine sans relecture quand rien n'est rejeté.** Lignes valides + rejetées = bronze :
   si l'historique Delta montre que le silver a reçu toutes les lignes, la quarantaine est
   réécrite vide sans lire le mois.
4. **Codes sans libellé contrôlés par dbt** (test `codes_sans_libelle`, avertissement), une fois
   pour tous les mois : 3,7 s pour deux mois sur DuckDB, contre 27 s par mois dans le silver.
5. **Statistiques Delta du silver limitées** aux colonnes de filtrage (mois, nature de
   prestation, région) au lieu des 32 premières : silver 269 s → 228 s.
6. **Propriétés de table appliquées aussi aux tables existantes** : Delta ne lit les options
   d'écriture qu'à la création ; `overwrite_month` fait un `ALTER TABLE` si la table existe.

## Résultats

Mêmes lignes et mêmes montants qu'avant, au centime, en local comme sur Databricks.

| Étape (par mois) | Local avant | Local après | Databricks avant | Databricks après |
|---|---:|---:|---:|---:|
| Bronze | 592 s / 510 s | **147 s / 130 s** | ~660 s | **130 s / 100 s** |
| Silver | 427 s / 407 s | **253 s / 226 s** | ~85 s | **58 s / 52 s** |
| Gold (deux mois) | 7 s | 15 s ¹ | ~90 s | 111 s ¹ |
| Job Databricks complet (deux mois) | | | ~28 min | **9 min** |

¹ inclut désormais le contrôle des codes sans libellé, retiré du silver.

## Conséquences

- La lecture du `.gz` n'est plus le goulot ; l'écriture du silver (typage et Parquet) l'est.
- Le dossier des fichiers bruts doit disposer de ~6 Go libres par mois pendant le chargement.
- Les tests d'intégration gold construisent leur silver une seule fois (90-114 s → 62 s).
