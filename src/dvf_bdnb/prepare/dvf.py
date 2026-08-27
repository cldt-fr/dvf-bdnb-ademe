"""Preparation du jeu DVF.

Ce qui est fait ici, et pourquoi :

- **Dedoublonnage par `id_mutation`.** Une vente portant plusieurs lots ou
  plusieurs locaux apparait sur autant de lignes, et `valeur_fonciere` repete a
  chaque fois le prix de la mutation ENTIERE. Sommer ou compter ces lignes
  telles quelles gonfle les volumes et fausse toutes les medianes. On regroupe
  donc par mutation en agregeant surfaces et pieces.
- **Filtrage des mutations sans valeur de marche** : seules les ventes comptent.
  Echanges, expropriations et adjudications ne refletent pas un prix negocie.
- **Typage** : tout arrive en texte dans le CSV. Le code commune corse `2A004`
  interdit de traiter les codes comme des entiers.
- **Prix au m2** calcule une fois pour toutes, et sa plausibilite signalee
  plutot que la ligne supprimee — c'est au consommateur de decider.

Ce qui n'est PAS fait ici : la geolocalisation. Etalab la publie deja dans
`geo-dvf`, et la refaire serait du travail perdu et moins fiable.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Bornes de plausibilite du prix au m2, en euros. Au-dela, la ligne est
# conservee mais marquee : une vente a 40 000 EUR/m2 existe a Paris, une a
# 50 EUR/m2 est presque toujours une mutation multi-biens mal decoupee.
MIN_PRICE_M2 = 500
MAX_PRICE_M2 = 30000

# Types de local DVF exploitables pour un prix au m2 : maison et appartement.
# Dependances et locaux professionnels n'ont pas de surface comparable.
DWELLING_TYPES = (1, 2)


def prepare(sources: list[Path], destination: Path, connection: duckdb.DuckDBPyConnection | None = None) -> dict:
    """Transforme des CSV `geo-dvf` en un Parquet prepare.

    `sources` peut couvrir plusieurs annees : elles sont consolidees en un seul
    jeu, la ou `geo-dvf` publie un fichier par annee ET par departement.
    """
    if not sources:
        raise ValueError("aucun fichier source a preparer")

    con = connection or duckdb.connect()
    destination.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB refuse les parametres lies dans un CREATE VIEW : la liste est donc
    # inlinee, avec echappement des apostrophes.
    files = ", ".join("'" + str(p).replace("'", "''") + "'" for p in sources)
    # Dialecte impose, jamais devine. DuckDB deduit le dialecte du PREMIER
    # fichier de la liste : si celui-la n'a aucun champ entre guillemets, il
    # conclut qu'il n'y en a pas, et le premier nom de voie contenant une
    # virgule — « RTE D'AIGRE , LES CHATELETS » — fait exploser le decoupage
    # sur un autre fichier. geo-dvf est du CSV standard : on le lui dit.
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW raw AS
        SELECT * FROM read_csv(
            [{files}],
            header = true, all_varchar = true,
            delim = ',', quote = '"', escape = '"'
        )
    """)

    _assert_shape(con)

    # Compte AVANT tout filtrage : sans lui, le rapport ne dit pas combien de
    # lignes la preparation ecarte, seulement combien elle en regroupe.
    lignes_source = con.execute("SELECT count(*) FROM raw").fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW mutations AS
        SELECT
            id_mutation,
            CAST(date_mutation AS DATE)                        AS date_mutation,
            CAST(strftime(CAST(date_mutation AS DATE), '%Y') AS SMALLINT) AS annee,
            nature_mutation,
            CAST(valeur_fonciere AS DOUBLE)                    AS valeur_fonciere,
            code_commune,
            nom_commune,
            code_postal,
            code_departement,
            -- Une mutation peut porter sur plusieurs parcelles : on garde la
            -- premiere par ordre stable, et leur nombre.
            min(id_parcelle)                                   AS id_parcelle,
            count(DISTINCT id_parcelle)                        AS nb_parcelles,
            -- Les surfaces, elles, s'additionnent sur les lignes de la mutation.
            sum(CAST(surface_reelle_bati AS DOUBLE))           AS surface_bati,
            sum(CAST(surface_terrain AS DOUBLE))               AS surface_terrain,
            max(CAST(nombre_pieces_principales AS INTEGER))    AS nb_pieces,
            count(*)                                           AS nb_lignes,
            -- Un type unique signale un bien homogene ; plusieurs types
            -- signalent une mutation composite dont le prix au m2 est douteux.
            count(DISTINCT code_type_local)                    AS nb_types,
            min(CAST(code_type_local AS TINYINT))              AS code_type_local,
            min(type_local)                                    AS type_local,
            avg(CAST(longitude AS DOUBLE))                     AS longitude,
            avg(CAST(latitude AS DOUBLE))                      AS latitude
        FROM raw
        WHERE nature_mutation = 'Vente'
          AND valeur_fonciere IS NOT NULL
        GROUP BY ALL
    """)

    con.execute(f"""
        COPY (
            SELECT
                *,
                CASE WHEN surface_bati > 0
                     THEN round(valeur_fonciere / surface_bati, 2)
                END AS prix_m2,
                -- Marque plutot que filtre : le consommateur tranche.
                CASE
                    WHEN surface_bati IS NULL OR surface_bati <= 0 THEN 'sans_surface'
                    WHEN code_type_local NOT IN {DWELLING_TYPES}   THEN 'type_non_logement'
                    WHEN nb_types > 1                              THEN 'mutation_composite'
                    WHEN valeur_fonciere / surface_bati < {MIN_PRICE_M2} THEN 'prix_m2_bas'
                    WHEN valeur_fonciere / surface_bati > {MAX_PRICE_M2} THEN 'prix_m2_haut'
                    ELSE 'ok'
                END AS qualite_prix_m2
            FROM mutations
            ORDER BY date_mutation, id_mutation
        ) TO '{destination}' (FORMAT parquet, COMPRESSION zstd)
    """)

    return _report(con, destination, lignes_source)


# Colonnes sans lesquelles la preparation n'a aucun sens.
REQUIRED_COLUMNS = (
    "id_mutation", "date_mutation", "nature_mutation", "valeur_fonciere",
    "code_commune", "id_parcelle", "surface_reelle_bati", "code_type_local",
    "longitude", "latitude",
)


def _assert_shape(con: duckdb.DuckDBPyConnection) -> None:
    """Verifie que la source a bien ete decoupee.

    Quand un CSV est mal forme — une ligne n'ayant pas le meme nombre de champs
    que l'en-tete, par exemple — DuckDB renonce au decoupage et renvoie une
    unique colonne contenant la ligne entiere. L'erreur qui suit parle alors
    d'une colonne manquante, ce qui envoie chercher au mauvais endroit. On le
    dit ici, clairement.
    """
    found = {row[0] for row in con.execute("DESCRIBE raw").fetchall()}
    missing = [c for c in REQUIRED_COLUMNS if c not in found]
    if not missing:
        return

    if len(found) <= 2:
        raise ValueError(
            f"la source n'a pas ete decoupee en colonnes ({len(found)} colonne(s) lue(s)). "
            "Le CSV est probablement mal forme : verifier que chaque ligne compte "
            "autant de champs que l'en-tete."
        )
    raise ValueError(
        "colonnes absentes de la source : " + ", ".join(missing) +
        ". Le format geo-dvf a peut-etre change."
    )


def _report(con: duckdb.DuckDBPyConnection, destination: Path, lignes_source: int) -> dict:
    """Comptes servant aux controles qualite bloquants de la publication."""
    row = con.execute(f"""
        SELECT
            count(*)                                              AS mutations,
            sum(nb_lignes)                                        AS lignes_retenues,
            count(*) FILTER (qualite_prix_m2 = 'ok')              AS exploitables,
            count(*) FILTER (longitude IS NOT NULL)               AS geolocalisees,
            min(date_mutation)                                    AS debut,
            max(date_mutation)                                    AS fin
        FROM read_parquet('{destination}')
    """).fetchone()

    mutations, retenues, ok, geo, debut, fin = row
    return {
        "mutations": mutations,
        "lignes_source": lignes_source,
        "lignes_retenues": retenues,
        "lignes_ecartees": lignes_source - retenues,
        "dedoublonnage": f"{lignes_source} lignes -> {retenues} retenues -> {mutations} mutations",
        "exploitables": ok,
        "part_exploitable": round(ok / mutations * 100, 1) if mutations else None,
        "part_geolocalisee": round(geo / mutations * 100, 1) if mutations else None,
        "periode": [str(debut), str(fin)],
    }
