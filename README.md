# dvf-bdnb-ademe

Les trois grands jeux publics du logement français — **DVF**, **BDNB** et **DPE** — nettoyés,
géolocalisés, typés, partitionnés par département, et republiés à chaque nouveau millésime.

Prêts à analyser tels quels, ou à charger en base en une commande.

## Pourquoi

Ces jeux sont publics et déjà mirrorés partout — mais **toujours bruts**.

DVF arrive avec une ligne par lot : une vente portant sur plusieurs parcelles occupe plusieurs
lignes, où le prix indiqué est celui de la vente **entière**. Qui compte les lignes quadruple le
nombre de ventes et quadruple les montants. Mesuré sur la Lozère : 46 744 lignes pour 11 706 ventes
réelles.

Le DPE compte 250 colonnes, et le champ `_geopoint` que l'ADEME publie pour situer les diagnostics
est **faux en outre-mer** : il traite toutes les coordonnées comme du Lambert 93, si bien que
Saint-Paul de La Réunion s'y retrouve en mer du Nord.

La BDNB est un dump de 90 tables et 40 Go, dont le schéma change de nom à chaque livraison — et dont
le stockage renvoie par moments des octets faux sans la moindre erreur HTTP.

Chaque réutilisateur refait donc le même travail ingrat, et retombe sur les mêmes pièges. Aucun n'est
documenté côté producteur.

Ce dépôt fait ce travail une fois, publiquement, à chaque livraison.

## Utiliser les données

Sans rien installer, directement depuis une Release :

```sql
SELECT nom_commune, median(prix_m2) AS prix_m2, count(*) AS ventes
FROM read_parquet('https://github.com/cldt-fr/dvf-bdnb-ademe/releases/download/2026-02-a/dvf/dept-33.parquet')
WHERE annee >= 2023 AND qualite_prix_m2 = 'ok'
GROUP BY 1 ORDER BY 2 DESC;
```

Ou en base, le DDL et le script de chargement étant fournis dans la Release :

```bash
psql -f schema/dvf.sql
zcat dvf/dept-33.csv.gz | psql -c "COPY dvf FROM STDIN (FORMAT csv, HEADER)"
```

## Reconstruire soi-même

```bash
uvx dvf-bdnb check                                    # de nouvelles données ?
uvx dvf-bdnb prepare --source dvf --departments 33
uvx dvf-bdnb prepare --source dpe --departments 974
```

## Un principe : signaler, pas supprimer

Une vente de terrain nu n'a pas de prix au m². Une mutation composite en a un, mais il ne veut rien
dire. Un diagnostic mal géocodé reste exploitable pour tout ce qui n'est pas spatial.

Ces lignes sont donc **conservées et marquées** (`qualite_prix_m2`, `qualite_position`), jamais
supprimées. C'est au consommateur de décider ce qu'il écarte — pas à nous de décider pour lui.

## État

En construction. Le socle, la détection de millésime et les préparations DVF et DPE fonctionnent.
Restent la BDNB, la publication et les contrôles qualité bloquants.

Le cahier des charges est dans [SPEC.md](./SPEC.md), avec la liste des pièges déjà identifiés et ce
que « préparé » signifie précisément pour chaque source.

## Licence

Code sous licence permissive. Données sous **Licence Ouverte 2.0** (Etalab) : réutilisation,
rediffusion et usage commercial autorisés sous réserve de mention de la source.

- **DVF** — DGFiP, géolocalisé par Etalab, via data.gouv.fr
- **BDNB** — CSTB, https://bdnb.io
- **DPE** — ADEME, https://data.ademe.fr
