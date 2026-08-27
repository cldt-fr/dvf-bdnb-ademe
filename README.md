# Les données du logement français, enfin utilisables

Trois jeux de données publics décrivent le parc immobilier français : **qui a vendu quoi et à quel
prix** (DVF), **de quoi les bâtiments sont faits** (BDNB), et **ce qu'ils consomment** (DPE).

Ils sont gratuits et accessibles à tous. Mais tels qu'ils sont publiés, ils sont difficiles à
exploiter : formats bruts, doublons, colonnes par centaines, coordonnées fausses par endroits.

Ce dépôt les nettoie et les republie, département par département, prêts à l'emploi.

> **État actuel : le code fonctionne, la première publication n'a pas encore eu lieu.**
> En attendant, la section « [Fabriquer les fichiers soi-même](#fabriquer-les-fichiers-soi-même) »
> permet de produire les données en quelques minutes.

---

## À quoi ça sert, concrètement

- Connaître le **prix réel au m²** dans une commune, à partir des ventes effectivement conclues —
  pas des prix demandés dans les annonces.
- Savoir **combien vaut une passoire thermique en moins** dans un secteur donné.
- Repérer les **bâtiments anciens et énergivores** d'un quartier.
- Alimenter une carte, une étude, un mémoire, un outil métier.

---

## Ce que vous récupérez

Un fichier par département et par jeu de données. Vous ne téléchargez que ce dont vous avez besoin :
la Gironde seule, pas la France entière.

| Fichier | Ce que c'est | Avec quoi l'ouvrir |
|---------|--------------|--------------------|
| `dept-33.csv.gz` | Un tableau, compressé | Excel, LibreOffice, un tableur, PostgreSQL |
| `dept-33.parquet` | Le même tableau, format compact | DuckDB, Python, R, QGIS |

**Si vous hésitez, prenez le `.csv.gz`.** C'est un fichier CSV compressé : votre outil de
décompression habituel l'ouvre, et vous obtenez un tableau lisible partout.

Le `.parquet` est cinq à dix fois plus léger et beaucoup plus rapide à interroger, mais il demande un
outil adapté. Il devient intéressant dès qu'on dépasse quelques centaines de milliers de lignes.

---

## Ouvrir les données

### Dans un tableur

Décompressez le `.csv.gz`, ouvrez le `.csv` obtenu.

⚠️ **Attention** : Excel s'arrête à environ un million de lignes. Pour un département dense comme le
Nord ou les Bouches-du-Rhône, le fichier dépasse cette limite et Excel le tronquera **sans
prévenir**. Dans ce cas, utilisez une des méthodes ci-dessous.

### Dans QGIS, pour cartographier

Les fichiers contiennent une colonne `latitude` et une colonne `longitude`. Dans QGIS :
*Couche → Ajouter une couche → Ajouter une couche de texte délimité*, en choisissant ces deux
colonnes comme coordonnées, et **WGS 84 (EPSG:4326)** comme système.

### Avec DuckDB, pour interroger sans rien installer de lourd

[DuckDB](https://duckdb.org/docs/installation/) est un outil en un seul fichier, gratuit, qui lit ces
données directement — y compris **depuis une adresse web, sans les télécharger**.

```sql
-- Prix médian au m² par commune de Gironde, sur les ventes récentes
SELECT nom_commune,
       median(prix_m2)::INT AS prix_m2,
       count(*)             AS ventes
FROM 'dept-33.parquet'
WHERE annee >= 2023
  AND qualite_prix_m2 = 'ok'     -- voir « Le champ le plus important » ci-dessous
GROUP BY 1
HAVING ventes >= 20              -- une médiane sur 3 ventes ne veut rien dire
ORDER BY prix_m2 DESC;
```

### Dans PostgreSQL

Chaque publication contient un fichier `schema/dvf.sql` avec la table déjà décrite et ses index.

```bash
psql -f schema/dvf.sql
zcat dept-33.csv.gz | psql -c "COPY dvf FROM STDIN (FORMAT csv, HEADER)"
```

Créez les index **après** le chargement : sur un gros département, cela divise le temps par deux.
Le fichier `.sql` le rappelle.

---

## Le champ le plus important : `qualite_prix_m2`

Toutes les ventes n'ont pas un prix au m² qui veut dire quelque chose. **Nous ne supprimons rien** —
nous marquons, et vous choisissez.

| Valeur | Ce que ça veut dire | À garder ? |
|--------|---------------------|-----------|
| `ok` | Un logement, une surface, un prix cohérent | **Oui** — c'est ce qu'il faut pour un prix au m² |
| `sans_surface` | Terrain nu, ou surface non renseignée | Non pour un prix au m², oui pour compter des ventes |
| `mutation_composite` | La vente mêle plusieurs types de biens | Non — le prix ne se rapporte pas à un seul bien |
| `type_non_logement` | Dépendance, local professionnel | Selon votre sujet |
| `prix_m2_bas` / `prix_m2_haut` | Valeur invraisemblable, souvent un découpage raté | Non |

**En pratique** : pour un prix au m², filtrez sur `qualite_prix_m2 = 'ok'`. Pour compter des ventes,
gardez tout.

Sans ce filtre, une vente de terrain à 8 €/m² et un garage à 40 000 €/m² se retrouvent dans votre
moyenne.

---

## Quatre choses à savoir avant d'utiliser DVF

**Une ligne = une vente.** Dans le fichier officiel, une vente portant sur plusieurs parcelles occupe
plusieurs lignes, où le prix affiché est celui de la vente **entière**. Compter les lignes revient à
quadrupler le nombre de ventes *et* les montants. Nous avons déjà regroupé : mesuré sur la Lozère,
46 744 lignes correspondent à 11 493 ventes réelles.

Le piège est plus retors qu'il n'y paraît : une vente peut porter sur des parcelles situées dans
**des communes différentes** (213 des 11 706 mutations de la Lozère, soit 1,8 %). Regrouper en
gardant le nom de commune la redécoupe en autant de lignes, chacune répétant le prix total. Nous
regroupons donc sur la vente seule, en retenant la localisation de la parcelle la plus bâtie et en
exposant `nb_communes` pour que le cas reste visible.

**`id_mutation` n'est pas un identifiant — utilisez `cle_vente`.** Le numéro fourni par DVF est une
séquence **réattribuée à chaque publication**. Vérifié sur la Lozère : les 9 457 lignes de 2021
portent un identifiant différent entre le millésime de 2023 et celui de 2025, décalé de 1 927,
alors que ce sont exactement les mêmes ventes. Un import incrémental calé sur `id_mutation`
reduplique donc tout le jeu à chaque millésime. Nous ajoutons `cle_vente`, calculée depuis le
contenu de la vente (date, prix, parcelles) et donc stable d'une publication à l'autre : c'est
elle qu'il faut prendre comme clé primaire.

**L'historique remonte à 2018, pas au-delà.** DVF est diffusé sur cinq années **glissantes** : la
DGFiP retire les années plus anciennes, et son propre fichier brut ne remonte pas plus loin que la
version géolocalisée. Nous complétons 2021-2025 par une archive communautaire couvrant 2018-2020,
validée contre la publication officielle sur leur année commune — 7 728 ventes appariées sur
(date, prix, parcelle, commune), 5 écarts d'un côté, 0 de l'autre. Deux limites en découlent :
cette archive ne couvre **que la métropole** (96 départements, Corse comprise), et là où les deux
sources se recouvrent, c'est **l'officiel qui prime**, vente par vente.

Avant 2018, il n'existe aucune source publique : les anciens millésimes de `files.data.gouv.fr`
répondent bien `200` mais sont **vides**, et l'archive du web n'a conservé que les listings, pas
les fichiers. Un bien absent du jeu n'est donc pas un bien jamais vendu — ne dites jamais à un
vendeur que son bien n'a jamais changé de mains.

Pour vous en tenir à la seule source officielle : `--sans-historique`.

**Trois départements manquent.** Le Bas-Rhin, le Haut-Rhin et la Moselle relèvent du livre foncier,
un système différent : DVF n'y publie rien. Mayotte non plus. Ce n'est pas une erreur de notre part.

---

## Deux choses à savoir sur le DPE

**Les coordonnées sont corrigées.** L'ADEME publie un champ de position qui est faux en outre-mer :
il place les diagnostics réunionnais en mer du Nord et les guadeloupéens dans le golfe de Guinée.
Nous recalculons les positions avec le système propre à chaque territoire.

**Un logement peut avoir plusieurs diagnostics.** Quand un DPE est refait, l'ancien reste dans le
fichier. La colonne `est_remplace` vaut `true` sur les versions périmées : filtrez sur
`est_remplace = false` pour n'avoir que les diagnostics en vigueur.

---

## Fabriquer les fichiers soi-même

Utile pour un département précis, ou tant que la première publication n'a pas eu lieu.

**Installation**, une seule fois. [uv](https://docs.astral.sh/uv/) est un installateur Python :

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh     # macOS et Linux
```

**Utilisation** — remplacez `33` par votre département :

```bash
uvx --from git+https://github.com/cldt-fr/dvf-bdnb-ademe dvf-bdnb \
    prepare --source dvf --departments 33
```

Le fichier apparaît dans `out/dvf/dept-33.parquet`. Comptez une minute pour un département moyen.

### Tout le jeu, d'un coup

```bash
dvf-bdnb run --jobs 8
```

Une seule commande qui enchaîne : détection des nouveautés → préparation de chaque source, tous
départements → contrôles qualité. Ajoutez `--millesime 2026-02-a` pour publier dans la foulée.

**Compter environ une heure et demie** pour tout : dix minutes pour les 97 départements DVF (220 Mo
produits), et le reste pour les 15,5 millions de diagnostics énergétiques.

**C'est reprenable.** Un territoire déjà produit est sauté : une exécution interrompue se relance
sans tout refaire. Utilisez `--force` pour recalculer malgré tout.

### Les commandes séparément

```bash
dvf-bdnb check                                   # de nouvelles données sont-elles parues ?
dvf-bdnb prepare --source dvf                    # tout le jeu DVF (défaut : « all »)
dvf-bdnb prepare --source dpe --departments 33   # un département précis
dvf-bdnb verify                                  # contrôler ce qu'on vient de produire
```

Les données de l'ADEME sont republiées **chaque semaine**, celles de DVF deux fois par an, la BDNB
trois fois. `check` vous dit ce qui a bougé, sans rien télécharger.

---

## Pourquoi faire confiance à ces fichiers

Parce qu'ils sont vérifiés avant d'être publiés, et que la publication est **refusée** si un contrôle
échoue :

- les positions doivent tomber dans leur territoire — le contrôle qui aurait détecté seul l'erreur
  de coordonnées de l'ADEME ;
- aucun département ne doit être vide, sauf ceux dont l'absence est attendue ;
- le volume ne doit pas s'effondrer d'un millésime à l'autre.

Chaque publication est accompagnée d'un fichier `manifest.json` contenant l'empreinte de chaque
fichier, pour vérifier qu'un téléchargement est arrivé intact.

Le détail de ce qui est fait sur chaque source, et pourquoi, est dans [SPEC.md](./SPEC.md).

---

## Poser une question, signaler une erreur

Ouvrez une [issue](https://github.com/cldt-fr/dvf-bdnb-ademe/issues). Une donnée qui vous semble
fausse est une information utile : ces jeux sont vastes et personne ne les a tous regardés.

---

## Licence

Le code est libre. Les données sont sous **Licence Ouverte 2.0** (Etalab) : vous pouvez les
réutiliser, les rediffuser et les exploiter commercialement, à condition de citer la source.

- **DVF** — Direction générale des Finances publiques, géolocalisé par Etalab
- **BDNB** — Centre Scientifique et Technique du Bâtiment
- **DPE** — Agence de la transition écologique
