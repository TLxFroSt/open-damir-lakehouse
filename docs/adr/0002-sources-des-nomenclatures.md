# ADR 0002 — Sources des nomenclatures

- Statut : accepté
- Date : 2026-10-03

## Contexte

Les 39 colonnes de codes du silver (nature de prestation, région, spécialité...) n'ont de sens
qu'avec leurs libellés. La source naturelle, le « Descriptif des variables » officiel
d'Open DAMIR, est inaccessible : le lien de l'Assurance Maladie et de data.gouv.fr renvoie une
erreur 404, depuis un navigateur comme depuis un script.

Deux sources ouvertes existent :

- les **nomenclatures SNDS** publiées par le Health Data Hub, à jour (avril 2026), qui couvrent
  les codes bruts (`PRS_NAT`, `EXO_MTF`, régions...) ;
- le **lexique Open DAMIR de 2015**, ancien mais propre à Open DAMIR, seul à décrire ses
  regroupements (tranches d'âge, spécialités regroupées...).

Mesuré sur janvier 2025, le lexique seul ne décode que 64 % des lignes pour `PRS_NAT`.

## Décisions

1. **Reconstituer les libellés** dans un fichier versionné, `reference/nomenclatures.csv`,
   généré par `damir.reference.nomenclatures` à partir des deux sources figées à un commit.
2. **Priorité choisie colonne par colonne**, après comparaison des libellés :
   SNDS d'abord pour les codes bruts (plus récent), lexique d'abord pour les regroupements
   Open DAMIR. Une même valeur peut avoir deux sens : `AGE_BEN_SNDS = 70` signifie
   « 70 - 79 ans » dans Open DAMIR, « 70 - 74 ans » dans la table SNDS des âges.
3. **Compléments minimaux et justifiés** pour les codes absents des deux sources ; un complément
   qui doublonnerait une source fait échouer la construction. Les libellés déduits sont marqués
   « à confirmer ».
4. **Codes conservés dans la table de faits**, libellés dans une table `nomenclatures` jointe à
   la lecture (couche gold). Un code sans libellé est signalé dans les logs du silver, jamais
   rejeté.

## Conséquences

- 36 colonnes sur 39 entièrement décodées, 99,95 % des valeurs de codes avec un libellé.
- Mettre à jour les libellés revient à changer un commit source et reconstruire : la différence
  apparaît dans le fichier versionné, relisible en revue.
- Si le descriptif officiel redevient accessible, il pourra devenir la source prioritaire sans
  changer le reste de la chaîne.
