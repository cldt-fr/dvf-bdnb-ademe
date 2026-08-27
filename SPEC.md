# Cahier des charges — `dvf-bdnb-ademe`

Chaine automatisee produisant, a chaque nouveau millesime, un **jeu de donnees derive** qui joint les
ventes immobilieres reelles, les caracteristiques du bati et les diagnostics energetiques — et le
publie pret a l'emploi.

Ce document decrit ce qu'on construit et pourquoi. Les gestes d'exploitation iront dans `RUNBOOK.md`.

---

## 1. Le probleme

Trois jeux publics decrivent le meme parc immobilier francais. Tous les trois sont diffuses **bruts**,
dans des formats et des etats qui imposent a chaque reutilisateur de refaire le meme travail ingrat
avant de pouvoir s'en servir.

| Jeu | Producteur | Volume brut | Cadence | Etat a la diffusion |
|-----|-----------|-------------|---------|---------------------|
| **DVF** | DGFiP, geolocalise par Etalab | ~18 M mutations, fenetre glissante ~5 ans | ~2x/an | Une ligne par LOT, tout en texte, un fichier par annee et par departement |
| **BDNB** | CSTB | ~32 M batiments, 40,5 Go compresses | 3x/an | Dump de 90 tables, schema nomme par millesime |
| **DPE** | ADEME | 15,5 M diagnostics depuis 07/2021 | mensuelle | ~250 colonnes, coordonnees en projections locales variables |

Chacun est deja mirrore un peu partout — mais **toujours brut**. Personne ne publie ces jeux
nettoyes, geolocalises, typaes et prets a charger.

Resultat : quiconque veut les exploiter recommence le meme parcours, et retombe sur les memes pieges.
Plusieurs nous ont coute des heures cette semaine (§6). Aucun n'est documente cote producteur.

**Le produit, c'est le travail de preparation — pas la donnee, qui est deja publique.**

## 2. Ce qu'on livre

Deux choses, un seul depot.

### 2.1 Trois jeux prepares, publies a chaque millesime

Un jeu par source, **partitionne par departement**, dans deux formats complementaires :

| Format | Pour quoi | Chargement |
|--------|-----------|------------|
| **Parquet** | Analyse directe, DuckDB, Python, R, QGIS | Aucun — lecture sur place, meme depuis une URL |
| **CSV compresse + DDL** | Chargement en base | `COPY` PostgreSQL, la voie la plus rapide en volume |

Chaque Release embarque le `CREATE TABLE`, les index recommandes et un script de chargement. Passer
de zero a une base exploitable doit tenir en une commande par departement, pas en une journee.

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
dvf-bdnb check                        # de nouvelles donnees sont-elles sorties ?
dvf-bdnb fetch --source bdnb          # telechargement verifie
dvf-bdnb prepare --source dvf --departments 33,40
dvf-bdnb publish --millesime 2026-02-a
```

### 2.3 En option : le jeu joint

La jointure des trois — une ligne par mutation, enrichie du batiment et de son diagnostic — n'existe
nulle part publiquement, et elle est **peu couteuse a produire** : la BDNB publie deja
`rel_batiment_groupe_parcelle.parcelle_id`, identifiant cadastral sur 14 caracteres qui est exactement
la cle des parcelles DVF. Jointure d'egalite, aucun calcul spatial.

A trancher : quatrieme jeu publie, ou hors perimetre. Voir §4, phase 5.

## 2 bis. Ce que « prepare » veut dire, source par source

C'est le cœur du projet : chaque traitement ci-dessous est un piege que le reutilisateur n'aura plus
a decouvrir seul.

### DVF

> **Deja fait par Etalab, ne pas le refaire** : la geolocalisation. `geo-dvf` publie `longitude` et
> `latitude` sur chaque ligne, par annee et par departement, depuis 2021. C'est cette source qu'on
> consomme — pas le DVF brut de la DGFiP.

- **Dedoublonnage par `id_mutation`.** Une vente portant plusieurs lots ou plusieurs locaux apparait
  sur autant de lignes. Comptee telle quelle, elle gonfle les volumes et fausse les medianes : le
  prix indique est celui de la mutation ENTIERE, pas du lot de la ligne. On regroupe donc par
  `id_mutation` en agregeant surfaces et pieces.
- **Consolidation multi-annees.** `geo-dvf` publie un fichier par annee ET par departement. On
  produit un jeu par departement couvrant toute la fenetre.
- **Mutations inexploitables ecartees** : nature autre que « Vente » (echanges, expropriations,
  adjudications ne refletent pas un prix de marche), et mutations multi-biens dont l'ecart entre
  surface du lot et surface batie depasse 30 % — leur prix au m2 n'a pas de sens.
- **Typage** : `valeur_fonciere` et surfaces en numerique, dates en date, codes en texte (le code
  commune corse `2A004` interdit l'entier).
- **Prix au m2 calcule et borne** aux valeurs plausibles, l'aberration etant signalee plutot que
  supprimee.
- **Couverture explicite** : DVF ne couvre ni le Bas-Rhin, ni le Haut-Rhin, ni la Moselle (livre
  foncier), ni Mayotte. Un departement vide doit se distinguer d'un departement absent.

### DPE (ADEME)

- **Reprojection en WGS 84.** L'ADEME publie les coordonnees dans le systeme metrique **local** de
  chaque territoire : Lambert 93 en metropole, UTM 20N aux Antilles, 22N en Guyane, 40S a La Reunion,
  38S a Mayotte. Les traiter uniformement place les diagnostics ultramarins hors de leur commune.
- **~250 colonnes ramenees a l'utile** : classes energie et GES, consommations, emissions, surface,
  type et periode de construction, adresse BAN, position, dates.
- **Dedoublonnage des DPE remplaces** : `numero_dpe_remplace` chaine les revisions ; sans traitement,
  un meme logement compte plusieurs fois.
- **Filtrage sur la qualite de geocodage** : en dessous d'un score BAN de 0,5, la position ne permet
  aucun rattachement fiable.

### BDNB

- **90 tables ramenees a une.** Le dump porte tout le modele CSTB ; on projette en une table par
  groupe de batiments : epoque de construction, materiaux, hauteur, niveaux, nombre et occupation des
  logements, ascenseur, usage, classe energetique representative.
- **Nom de schema resolu, jamais suppose.** Le dump cree un schema nomme d'apres le millesime
  (`bdnb_2026_02_a_open_data`), qui change a chaque livraison.
- **Geometries en WGS 84**, le SRID etant detecte et non presume.
- **Relations conservees** : parcelle et DPE, qui sont ce qui rend la jointure possible en aval.

## 3. Choix techniques

### 3.1 DuckDB, pas PostgreSQL

Le reflexe serait de restaurer la BDNB dans PostGIS. C'est ce qu'on a fait ailleurs, et ca coute
**plusieurs heures de restauration pour 40 Go**, plus autant d'espace disque.

Or la BDNB publie aussi un export **CSV (39,4 Go)** que DuckDB lit directement, sans import. Ajoute
le format colonnaire, l'elagage de partitions et l'extension spatiale, et la meme charge passe de
« plusieurs heures » a « quelques minutes de scan », sur une machine modeste.

C'est ce qui rend l'outil executable par d'autres, et pas seulement par nous.

### 3.2 Rust

Un binaire statique unique, sans environnement a installer — condition pour que « chacun l'execute
chez soi » soit vrai et pas theorique. DuckDB est embarque via la crate `duckdb`, donc le gros du
calcul ne depend pas du langage hote.

Le telechargeur resilient (§3.3) est precisement le genre de code qui gagne a etre ecrit dans un
langage ou l'erreur est dans le type de retour.

> Alternative ecartee : Python + DuckDB, plus accessible aux contributeurs du monde geo, mais impose
> un environnement a chaque utilisateur et complique la distribution. A reconsiderer si l'objectif
> devient la contribution externe plutot que la diffusion.

### 3.3 Un telechargeur qui ne fait confiance a personne

**Constate en conditions reelles** : le stockage objet qui sert la BDNB renvoie par moments des
octets faux sur les grosses lectures, **sans erreur HTTP** — code 200, longueur correcte, contenu
different. Trois lectures successives de la meme plage de 64 Mo ont donne trois empreintes
differentes. Deux telechargements complets d'affilee ont produit des archives corrompues.

Pour un pipeline automatique, c'est le risque numero un : **publier des donnees fausses tout seul est
bien pire que ne rien publier**. Le telechargeur lit donc par tronçons, chacun telecharge deux fois,
accepte seulement si les deux lectures concordent, et relance le tronçon sinon — jamais les 40 Go.
Le controle d'integrite final reste le juge.

### 3.4 Detection de millesime, pas notification

Aucun des trois producteurs ne publie de webhook ni de flux. « Des la sortie » se traduit donc par un
sondage quotidien qui compare `ETag`, `Last-Modified`, taille et empreinte a un etat connu, et ne
declenche que sur changement reel. Latence de detection : moins de 24 h.

### 3.5 Calcul hors de GitHub

Les runners GitHub offrent 14 Go de disque et 6 h de limite. La BDNB en fait 40 a elle seule. Le
depot porte le code et l'orchestration ; le calcul tourne sur un **runner self-hoste**.

---

## 4. Phases

### Phase 0 — Socle
- Squelette Rust, CI, licences (code permissif, donnees sous Licence Ouverte 2.0 avec attribution).
- `sources.toml` : registre declaratif des sources (URL, format, cadence, strategie d'empreinte).
- Telechargeur resilient (§3.3) + `state.json` des millesimes connus.

### Phase 1 — `check`
Sondage des trois sources, comparaison a l'etat connu, code de sortie exploitable par un cron
quotidien sur le runner self-hoste.

### Phase 2 — `prepare --source dvf`
La plus utile, et la plus facile a eprouver : un departement peu peuple pese quelques centaines de
kilo-octets, donc la chaine complete se teste en quelques secondes. Dedoublonnage par `id_mutation`,
filtrage des mutations inexploitables, typage, prix au m2, consolidation multi-annees.

### Phase 3 — `prepare --source dpe`
La plus frequente : mensuelle, purement tabulaire. Reprojection par territoire, selection de colonnes,
dedoublonnage des revisions.

### Phase 4 — `prepare --source bdnb`
La plus lourde : 40 Go a lire, 90 tables a projeter en une. C'est ici que le choix DuckDB paie, et
ici que le telechargeur resilient est indispensable.

### Phase 5 — `publish`
Release GitHub par millesime, un asset par departement et par source, `manifest.json` (SHA-256,
comptes de lignes, couverture par departement) et `SCHEMA.md`. Le depot ne contient jamais de
donnees : code, schema, manifeste.

### Phase 6 — Controles qualite, **bloquants**
Comptes par departement, distributions, taux de geolocalisation, ecart au millesime precedent.
**Une regression bloque la publication.** C'est la contrepartie indispensable de l'automatisation :
sans ce garde-fou, la chaine publierait ses propres erreurs a heure fixe, avec l'autorite d'un jeu
officiel.

### Phase 7 — En option : le jeu joint
Voir §2.3. A decider une fois les trois jeux stabilises — la jointure est peu couteuse, mais elle
engage sur une semantique (quel batiment pour quelle parcelle, quel DPE pour quelle vente) qu'il vaut
mieux ne pas figer trop tot.

## 5. Ce que ca change ailleurs

Le serveur MCP « donnee immobiliere » de MeilleursBiens ingere aujourd'hui la BDNB et les DPE
directement en production : 40 Go a restaurer sur la machine qui sert l'API, decouverte de schema a
chaque millesime, garde-fou disque pour eviter de remplir le volume.

S'il chargeait ces jeux prepares a la place, tout cela disparait — et le chargement se reduit a un
`COPY` par departement. Cette simplification est un objectif du projet, pas un effet de bord.

## 6. Pieges deja payes

Ils sont ici pour que personne ne les repaye.

| Piege | Consequence |
|-------|-------------|
| Source qui corrompt sans erreur HTTP | Deux archives de 40 Go inutilisables avant diagnostic |
| `wget -c` sur un fichier corrompu de taille exacte | « already fully retrieved » — la corruption survit a la reprise |
| wget ne reessaie pas sur HTTP 5xx par defaut | Abandon silencieux sur un 500 transitoire, fichier vide |
| Nom du schema BDNB indexe sur le millesime | Code en dur = tout casse a la livraison suivante, sans erreur |
| Coordonnees ADEME en systeme metrique local | Diagnostics ultramarins projetes hors de leur territoire |
| Cast `geography` sur une colonne indexee | L'index GiST n'est plus utilise, la passe devient injouable |
| DVF en fenetre glissante, pas historique complet | Conclure a tort qu'un bien n'a jamais ete vendu |

---

## 7. Sources et licence

Donnees sous **Licence Ouverte 2.0** (Etalab) : reutilisation, rediffusion et usage commercial
autorises sous reserve de mention de la source.

- DVF — DGFiP, via data.gouv.fr
- BDNB — CSTB, https://bdnb.io
- DPE — ADEME, https://data.ademe.fr
