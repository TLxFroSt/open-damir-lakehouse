# ADR 0001 — Règles de la couche silver

- Statut : accepté
- Date : 2026-10-03

## Contexte

Le bronze contient les fichiers mensuels Open DAMIR tels quels : 56 colonnes en texte,
environ 35 millions de lignes par mois. Chaque ligne est un **agrégat** (montants et
dénombrements cumulés pour une combinaison de codes), pas un acte individuel : elle n'a
pas de clé unique.

Le profilage de janvier 2025 (36,6 M de lignes) a montré :

- des montants sans zéro initial (`.51`, `-.31`) mais tous convertibles ;
- des montants négatifs nombreux (5,6 M de remboursements négatifs) : ce sont des régularisations ;
- 15 607 lignes avec un mois des soins inconnu (`SOI_ANN = 0000`, `SOI_MOI = 00`) ;
- des codes de nomenclature absents du lexique de 2015 (`PRS_NAT` : 64 % des lignes couvertes).

## Décisions

1. **Pas de dédoublonnage.** Deux lignes identiques peuvent être deux agrégats légitimes ;
   les supprimer ferait disparaître de la dépense réelle. Les doublons de *chargement* sont
   déjà impossibles : chaque mois remplace sa propre partition (bronze comme silver).

2. **Quarantaine plutôt que suppression.** Une ligne est mise en quarantaine, avec ses valeurs
   d'origine en texte et la liste de ses motifs, si :
   - une valeur numérique n'est pas convertible (`non_numerique:<CODE>`) ;
   - `FLX_ANN_MOI` ne correspond pas au mois de la partition (`mois_traitement_incoherent`) ;
   - le mois des soins est impossible (`mois_soins_invalide`) ou postérieur au mois de
     traitement (`soins_apres_traitement`).

   La quarantaine d'un mois est réécrite à chaque exécution, même vide : un mois corrigé
   n'y laisse pas d'anciens rejets.

3. **Ce qui n'est pas un rejet.**
   - Mois des soins inconnu (0000/00) : ligne conservée, `mois_soins` nul. La dépense est réelle.
   - Code de nomenclature inconnu : ligne conservée. Les nomenclatures évoluent chaque année ;
     rejeter ces lignes fausserait les totaux. Leur suivi relève du décodage des nomenclatures.
   - Montant négatif : régularisation, conservé tel quel.

4. **Types.** Montants et coefficients en `decimal(18,2)` (jamais de flottant pour de l'argent),
   dénombrements en `int`, taux en `decimal(6,2)`, mois en `date` (1er du mois). Les codes de
   nomenclature restent en texte : ce sont des catégories, et certains ont des zéros significatifs.

5. **Conversion contrôlée.** Spark 4 fonctionne en mode ANSI : un `cast` invalide arrête le job.
   Le silver utilise `try_cast` (valeur nulle si non convertible) et compare chaque valeur
   convertie à sa source pour détecter les rejets.

6. **Contrôle de cohérence à chaque exécution.** Après écriture, on relit le silver et la
   quarantaine du mois : leurs nombres de lignes et leurs montants de dépense (`PRS_PAI_MNT`)
   doivent redonner exactement ceux du bronze, sinon le job échoue (`ReconciliationError`).

7. **Noms de colonnes en français.** Le vocabulaire métier (ticket modérateur, dépassement,
   base de remboursement) n'a pas d'équivalent anglais exact. La correspondance avec les codes
   DAMIR est générée dans [`docs/colonnes_silver.md`](../colonnes_silver.md).

## Conséquences

- Les totaux du silver sont comparables aux publications officielles.
- La quarantaine est presque toujours vide sur des fichiers sains ; elle protège contre un
  fichier corrompu ou un changement de format.
- Le contrôle de cohérence relit les tables écrites : quelques dizaines de secondes de plus
  par mois, en échange d'une garantie vérifiée plutôt que supposée.

## Premier constat (janvier et février 2025)

71 350 482 lignes en silver, 13 en quarantaine, cohérence vérifiée sur les deux mois.
Les 13 lignes rejetées ont toutes un mois des soins en janvier de l'an 0001
(`SOI_ANN = 0001`, `SOI_MOI = 01`) : probablement une autre valeur « date inconnue »,
non documentée, en plus de 0000/00.
