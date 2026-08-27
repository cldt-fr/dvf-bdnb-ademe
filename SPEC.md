# Cahier des charges — `dvf-bdnb-ademe`

Chaîne automatisée qui, à chaque nouveau millésime, prépare les trois grands jeux publics du logement
français et les republie prêts à l'emploi.

Ce document décrit ce qu'on construit et pourquoi. Les gestes d'exploitation iront dans `RUNBOOK.md`.

---

## 1. Le problème

Trois jeux publics décrivent le même parc immobilier français. Tous les trois sont diffusés **bruts**,
dans des formats et des états qui imposent à chaque réutilisateur de refaire le même travail ingrat
avant de pouvoir s'en servir.

| Jeu | Producteur | Volume brut | Cadence | État à la diffusion |
|-----|-----------|-------------|---------|---------------------|
| **DVF** | DGFiP, géolocalisé par Etalab | ~18 M mutations, fenêtre glissante ~5 ans | ~2×/an | Une ligne par lot, tout en texte, un fichier par année et par département |
| **BDNB** | CSTB | ~32 M bâtiments, 40,5 Go compressés | 3×/an | Dump de 90 tables, schéma nommé d'après le millésime |
| **DPE** | ADEME | 15,5 M diagnostics depuis 07/2021 | mensuelle | ~250 colonnes, coordonnées géographiques fausses en outre-mer |

Chacun est déjà mirroré un peu partout — mais **toujours brut**. Personne ne publie ces jeux
nettoyés, géolocalisés, typés et prêts à charger.

Résultat : quiconque veut les exploiter recommence le même parcours et retombe sur les mêmes pièges.
Plusieurs nous ont coûté des heures (§6). Aucun n'est documenté côté producteur.

**Le produit, c'est le travail de préparation — pas la donnée, qui est déjà publique.**

---

## 2. Ce qu'on livre

Deux choses, un seul dépôt.

### 2.1 Trois jeux préparés, publiés à chaque millésime

Un jeu par source, **partitionné par département**, dans deux formats complémentaires :

| Format | Pour quoi | Chargement |
|--------|-----------|------------|
| **Parquet** | Analyse directe : DuckDB, Python, R, QGIS | Aucun — lecture sur place, même depuis une URL |
| **CSV compressé + DDL** | Mise en base | `COPY` PostgreSQL, la voie la plus rapide en volume |

Chaque Release embarque le `CREATE TABLE`, les index recommandés et la commande de chargement.
Passer de zéro à une base exploitable doit tenir en une commande par département, pas en une journée.

Le DDL est **dérivé du schéma Parquet**, jamais écrit à la main : une colonne ajoutée en amont se
retrouve dans le `CREATE TABLE` sans que personne y pense, et les types ne peuvent pas diverger du
fichier livré. Les index ne sont émis que pour les colonnes réellement présentes — un index sur une
colonne absente ferait échouer tout le script.

`manifest.json` porte les empreintes SHA-256 de chaque fichier. Ce n'est pas une précaution de
principe : la source amont s'est révélée capable de renvoyer des octets faux sans erreur HTTP, et un
consommateur doit pouvoir distinguer un fichier intact d'un fichier abîmé en transit.

```
releases/2026-02-a/
  dvf/dept-33.parquet     dvf/dept-33.csv.gz     schema/dvf.sql
  dpe/dept-33.parquet     dpe/dept-33.csv.gz     schema/dpe.sql
  bdnb/dept-33.parquet    bdnb/dept-33.csv.gz    schema/bdnb.sql
  manifest.json           SCHEMA.md
```

### 2.2 Un outil

Une CLI autonome qui reconstruit tout, ou une partie, chez soi.

```
dvf-bdnb check                        # de nouvelles données sont-elles sorties ?
dvf-bdnb prepare --source dvf --departments 33,40
dvf-bdnb publish --millesime 2026-02-a
```

### 2.3 En option : le jeu joint

La jointure des trois — une ligne par mutation, enrichie du bâtiment et de son diagnostic — n'existe
nulle part publiquement, et elle est **peu coûteuse à produire** : la BDNB publie déjà
`rel_batiment_groupe_parcelle.parcelle_id`, identifiant cadastral sur 14 caractères qui est exactement
la clé des parcelles DVF. Jointure d'égalité, aucun calcul spatial.

À trancher : quatrième jeu publié, ou hors périmètre. Voir §4, phase 7.

---

## 3. Ce que « préparé » veut dire, source par source

C'est le cœur du projet : chaque traitement ci-dessous est un piège que le réutilisateur n'aura plus
à découvrir seul.

### DVF

> **Déjà fait par Etalab, ne pas le refaire** : la géolocalisation. `geo-dvf` publie `longitude` et
> `latitude` sur chaque ligne, par année et par département, depuis 2021. C'est cette source qu'on
> consomme — pas le DVF brut de la DGFiP.

- **Dédoublonnage par `id_mutation`.** Une vente portant sur plusieurs lots ou plusieurs locaux
  apparaît sur autant de lignes, et `valeur_fonciere` y répète le prix de la vente **entière**.
  Comptée telle quelle, elle gonfle les volumes et fausse toutes les médianes. On regroupe donc par
  mutation, en additionnant les surfaces. *Mesuré sur la Lozère : 46 744 lignes → 11 706 mutations.*
- **Consolidation multi-années.** `geo-dvf` publie un fichier par année **et** par département ; on
  produit un jeu par département couvrant toute la fenêtre.
- **Mutations sans valeur de marché écartées** : seules les ventes comptent. Échanges,
  expropriations et adjudications ne reflètent pas un prix négocié.
- **Typage** : tout arrive en texte. Le code commune corse `2A004` interdit de traiter les codes
  comme des entiers.
- **Prix au m² calculé**, et sa plausibilité **signalée** plutôt que la ligne supprimée.
- **Couverture explicite** : DVF ne couvre ni le Bas-Rhin, ni le Haut-Rhin, ni la Moselle (livre
  foncier), ni Mayotte. Un département vide doit se distinguer d'un département absent.

### DPE (ADEME)

- **Reprojection en WGS 84.** L'ADEME publie les coordonnées dans le système métrique **local** de
  chaque territoire : Lambert 93 en métropole, UTM 20N aux Antilles, 22N en Guyane, 40S à La Réunion,
  38S à Mayotte.

  Elle expose bien un champ `_geopoint` censé donner des coordonnées géographiques, **mais il est
  faux en outre-mer** : il traite toutes les coordonnées comme du Lambert 93. Vérifié sur la source
  le 27/08/2026 — Saint-Paul de La Réunion y tombe à la latitude 56 (mer du Nord) et
  Capesterre-Belle-Eau à la latitude 6 (golfe de Guinée). On reprojette donc soi-même. *Après
  correction, La Réunion s'étend de -21,38 à -20,88 de latitude : son emprise réelle.*

  Second piège, dans la reprojection elle-même : avec EPSG:4326, PROJ suit l'ordre d'axe officiel
  (latitude, longitude). Sans `always_xy`, `ST_X` renvoie la latitude et tout le jeu sort de travers,
  sans aucune erreur. Les deux pièges sont verrouillés par des tests.

- **~250 colonnes ramenées à l'utile.**
- **Marquage des diagnostics remplacés** : `numero_dpe_remplace` chaîne les révisions ; sans
  traitement, un logement révisé compte autant de fois qu'il a été diagnostiqué.
- **Qualité de géocodage signalée**, pas filtrée.

### BDNB

- **90 tables ramenées à une.** On projette une ligne par groupe de bâtiments : époque de
  construction, matériaux, hauteur, niveaux, nombre et occupation des logements, usage, classe
  énergétique représentative. On consomme l'**export CSV**, que DuckDB lit directement — et on
  n'extrait de l'archive que les cinq tables utiles, en un seul passage sur le flux, sans écrire les
  39 Go sur disque.
- **Une parcelle, un bâtiment.** Une parcelle porte souvent plusieurs constructions (cour,
  dépendances, immeubles multiples). Sans arbitrage, une vente serait comptée autant de fois qu'il y
  a de bâtiments sur son terrain. On retient la parcelle principale marquée par le CSTB d'abord,
  puis, à défaut, le bâtiment le plus habité.
- **Format de géométrie détecté, pas supposé.** Selon l'export, `geom_groupe` arrive en WKT ou en
  WKB hexadécimal. Choisir au hasard produirait soit une erreur, soit — bien pire — une colonne
  entièrement nulle sans le moindre message.
- **Tables d'enrichissement facultatives.** L'absence du DPE représentatif ou de la BD TOPO est
  signalée dans le rapport, pas fatale : le jeu se produit quand même.
- **Nom de schéma résolu, jamais supposé.** Le dump crée un schéma nommé d'après le millésime
  (`bdnb_2026_02_a_open_data`), qui change à chaque livraison.
- **Géométries en WGS 84**, le SRID étant détecté et non présumé.
- **Relations conservées** : parcelle et DPE, qui sont ce qui rend la jointure possible en aval.

---

## 4. Choix techniques

### 4.1 DuckDB, pas PostgreSQL

Le réflexe serait de restaurer la BDNB dans PostGIS. C'est ce qu'on fait ailleurs, et cela coûte
**plusieurs heures de restauration pour 40 Go**, plus autant d'espace disque.

Or la BDNB publie aussi un export **CSV (39,4 Go)** que DuckDB lit directement, sans import. Ajoutez
le format colonnaire, l'élagage de partitions et l'extension spatiale, et la même charge passe de
« plusieurs heures » à « quelques minutes de scan », sur une machine modeste.

C'est ce qui rend l'outil exécutable par d'autres, et pas seulement par nous.

### 4.2 Python

Le gros du calcul revient à DuckDB : le langage hôte ne porte que l'orchestration et le réseau. Rust
aurait donné un binaire unique, mais au prix d'une chaîne de compilation à installer — et `uvx`
distribue tout aussi bien, sans rien demander à l'utilisateur.

### 4.3 Un téléchargeur qui ne fait confiance à personne

**Constaté en conditions réelles** : le stockage objet qui sert la BDNB renvoie par moments des
octets faux sur les grosses lectures, **sans erreur HTTP** — code 200, longueur correcte, contenu
différent. Trois lectures successives de la même plage de 64 Mo ont donné trois empreintes
différentes. Deux téléchargements complets d'affilée ont produit des archives corrompues.

Pour un pipeline automatique, c'est le risque numéro un : **publier des données fausses tout seul est
bien pire que ne rien publier**. Le téléchargeur lit donc par tronçons, chacun téléchargé deux fois,
et n'accepte que si les deux lectures concordent — un désaccord relance le tronçon, jamais les 40 Go.
Le contrôle d'intégrité final reste le juge.

### 4.4 Détection de millésime, pas notification

Aucun des trois producteurs ne publie de webhook ni de flux. « Dès la sortie » se traduit donc par un
sondage quotidien qui compare `ETag`, `Last-Modified`, taille et empreinte à un état connu, et ne
déclenche que sur changement réel. Latence de détection : moins de 24 h.

Une source exposée par API n'a pas d'`ETag` : pour le DPE, la marque de version est le couple date de
mise à jour et nombre de lignes.

### 4.5 Calcul hors de GitHub

Les runners GitHub offrent 14 Go de disque et 6 h de limite. La BDNB en fait 40 à elle seule. Le
dépôt porte le code et l'orchestration ; le calcul tourne sur un **runner auto-hébergé**.

---

## 5. Phases

| Phase | Objet | État |
|-------|-------|------|
| 0 | Socle : registre déclaratif, état des millésimes, téléchargeur vérifié | ✅ |
| 1 | `check` — détection de nouvelle version sur les trois sources | ✅ |
| 2 | `prepare --source dvf` | ✅ |
| 3 | `prepare --source dpe` | ✅ |
| 4 | `prepare --source bdnb` | ✅ |
| 5 | `publish` — Releases, manifeste, DDL | ✅ |
| 6 | Contrôles qualité **bloquants** | ✅ |
| 7 | En option : le jeu joint (§2.3) | à décider |

**Phase 6, la plus importante.** `verify` passe chaque jeu au crible et sort en code 1 si un contrôle
bloque ; `publish` refuse de partir dans ce cas. C'est la contrepartie indispensable de
l'automatisation : sans ce garde-fou, la chaîne publierait ses propres erreurs à heure fixe, avec
l'autorité d'un jeu de référence.

Trois familles de contrôles :

- **Volume** — un département vide bloque, sauf là où l'absence est attendue : le Bas-Rhin, le
  Haut-Rhin, la Moselle et Mayotte sont hors DVF (livre foncier). Confondre cette absence avec une
  panne ferait bloquer une publication valide tous les mois, pour toujours.
- **Emprise géographique** — chaque territoire a la sienne. C'est le contrôle qui aurait attrapé
  seul le `_geopoint` de l'ADEME, sans qu'aucun humain ait à regarder une carte. Éprouvé sur les
  données réelles : sur les coordonnées telles que l'ADEME les publie pour La Réunion, il rend
  *« 100 % des points hors du territoire — reprojection probablement fausse »* et refuse la
  publication. Quelques points isolés donnent un simple avertissement : une saisie erronée n'est
  pas une reprojection ratée.
- **Régression** — comparaison au millésime précédent, lue dans son manifeste publié. Une perte de
  plus de 20 % des lignes bloque : c'est un problème amont, pas une évolution.

La référence vient du **manifeste de la Release précédente**, pas d'un fichier d'état local : un
manifeste dit ce qui a réellement été publié et ne peut pas diverger de la réalité.

---

## 6. Pièges déjà payés

Ils sont ici pour que personne ne les repaye.

| Piège | Conséquence |
|-------|-------------|
| Une ligne DVF par lot, prix de la vente entière répété | Ventes et montants multipliés par ~4 |
| `_geopoint` de l'ADEME calculé en Lambert 93 partout | Diagnostics ultramarins à des milliers de kilomètres |
| EPSG:4326 en ordre d'axe officiel | Latitude et longitude interverties, sans erreur |
| Source qui corrompt sans erreur HTTP | Deux archives de 40 Go inutilisables avant diagnostic |
| `wget -c` sur un fichier corrompu de taille exacte | « already fully retrieved » — la corruption survit à la reprise |
| wget ne réessaie pas sur HTTP 5xx par défaut | Abandon silencieux sur un 500 transitoire, fichier vide |
| Nom du schéma BDNB indexé sur le millésime | Code en dur = tout casse à la livraison suivante, sans erreur |
| CSV mal formé lu par DuckDB | Une seule colonne, et un message qui envoie chercher ailleurs |
| DVF en fenêtre glissante, pas historique complet | Conclure à tort qu'un bien n'a jamais été vendu |

---

## 7. Sources et licence

Données sous **Licence Ouverte 2.0** (Etalab) : réutilisation, rediffusion et usage commercial
autorisés sous réserve de mention de la source.

- **DVF** — DGFiP, géolocalisé par Etalab, via data.gouv.fr
- **BDNB** — CSTB, https://bdnb.io
- **DPE** — ADEME, https://data.ademe.fr
