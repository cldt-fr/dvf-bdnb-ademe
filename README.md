# dvf-bdnb-ademe

Les trois grands jeux publics du logement francais — **DVF**, **BDNB** et **DPE** — nettoyes,
geolocalises, typaes, partitionnes par departement, et republies a chaque nouveau millesime.

Prets a analyser tels quels, ou a charger en base en une commande.

## Pourquoi

Ces jeux sont publics et deja mirrores partout — mais **toujours bruts**. DVF arrive sans
coordonnees et avec ses doublons de lots. Le DPE compte 250 colonnes et des coordonnees en
projections locales differentes selon le territoire. La BDNB est un dump de 90 tables et 40 Go.

Chaque reutilisateur refait donc le meme travail ingrat, et retombe sur les memes pieges — dont
aucun n'est documente cote producteur.

Ce depot fait ce travail une fois, publiquement, a chaque livraison.

## Utiliser les donnees

Sans rien installer, directement depuis une Release :

```sql
SELECT commune, median(prix_m2) AS prix_m2
FROM read_parquet('https://github.com/cldt-fr/dvf-bdnb-ademe/releases/download/2026-02-a/dvf/dept-33.parquet')
WHERE annee_mutation >= 2023
GROUP BY 1 ORDER BY 2 DESC;
```

Ou en base, le DDL et le script de chargement etant fournis dans la Release :

```bash
psql -f schema/dvf.sql
zcat dvf/dept-33.csv.gz | psql -c "COPY dvf FROM STDIN (FORMAT csv, HEADER)"
```

## Reconstruire soi-meme

```bash
dvf-bdnb check                                   # de nouvelles donnees ?
dvf-bdnb prepare --source dvf --departments 33
```

## Etat

En conception. Le cahier des charges est dans [SPEC.md](./SPEC.md), y compris la liste des pieges
deja identifies et ce que « prepare » signifie precisement pour chaque source.

## Licence

Code sous licence permissive. Donnees sous **Licence Ouverte 2.0** (Etalab) : reutilisation,
rediffusion et usage commercial autorises sous reserve de mention de la source.

- **DVF** — DGFiP, via data.gouv.fr
- **BDNB** — CSTB, https://bdnb.io
- **DPE** — ADEME, https://data.ademe.fr
