"""Preparation du jeu DPE (ADEME).

Ce qui est fait ici, et pourquoi :

- **Reprojection depuis les coordonnees metriques locales.** L'ADEME publie un
  champ `_geopoint` cense donner des coordonnees geographiques — mais il est
  **faux en outre-mer** : il traite toutes les coordonnees comme du Lambert 93.
  Verifie sur la source : Saint-Paul de La Reunion y tombe en mer du Nord, et
  Capesterre-Belle-Eau dans le golfe de Guinee. On reprojette donc nous-memes
  depuis `coordonnee_cartographique_*_ban`, avec le systeme propre a chaque
  territoire.
- **~250 colonnes ramenees a l'utile.**
- **Dedoublonnage des revisions** : `numero_dpe_remplace` chaine les versions
  successives d'un meme diagnostic. Sans traitement, un logement revise compte
  autant de fois qu'il a ete diagnostique.
- **Qualite de geocodage signalee**, pas filtree — comme pour DVF, c'est au
  consommateur de trancher.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# L'ADEME publie les coordonnees dans le systeme metrique LOCAL de chaque
# territoire. Les traiter uniformement en Lambert 93 — ce que fait son propre
# `_geopoint` — place les diagnostics ultramarins a des milliers de kilometres.
METRIC_SRID = 2154  # Lambert 93, métropole et Corse

# Chaque territoire a son système. Tous ceux-ci ont été vérifiés en reprojetant
# une coordonnée réelle de l'ADEME et en contrôlant qu'elle retombe bien sur le
# territoire.
OVERSEAS_SRID = {
    "971": 5490,  # Guadeloupe — RGAF09 / UTM 20N
    "972": 5490,  # Martinique — RGAF09 / UTM 20N
    "973": 2972,  # Guyane — RGFG95 / UTM 22N
    "974": 2975,  # La Réunion — RGR92 / UTM 40S
    "975": 4467,  # Saint-Pierre-et-Miquelon — RGSPM06 / UTM 21N
    "976": 4471,  # Mayotte — RGM04 / UTM 38S
    "977": 5490,  # Saint-Barthélemy — RGAF09 / UTM 20N
    "978": 5490,  # Saint-Martin — RGAF09 / UTM 20N
    "988": 3163,  # Nouvelle-Calédonie — RGNC91-93 / Lambert NC
}

# Départements où les coordonnées sont en Lambert 93. Énumérés plutôt que
# déduits : un territoire inconnu doit lever une erreur, pas retomber
# silencieusement sur la métropole.
METROPOLITAN = frozenset(
    [f"{n:02d}" for n in range(1, 96) if n != 20] + ["2A", "2B"]
)

# En dessous, la position BAN est trop incertaine pour rattacher le diagnostic
# a quoi que ce soit.
MIN_BAN_SCORE = 0.5

COLUMNS = (
    "numero_dpe",
    "numero_dpe_remplace",
    "date_etablissement_dpe",
    "date_fin_validite_dpe",
    "etiquette_dpe",
    "etiquette_ges",
    "conso_5_usages_par_m2_ep",
    "emission_ges_5_usages_par_m2",
    "surface_habitable_logement",
    "type_batiment",
    "annee_construction",
    "periode_construction",
    "adresse_ban",
    "code_postal_ban",
    "code_insee_ban",
    "code_departement_ban",
    "identifiant_ban",
    "coordonnee_cartographique_x_ban",
    "coordonnee_cartographique_y_ban",
    "score_ban",
    "statut_geocodage",
)


def srid_for(department: str) -> int:
    """Système métrique source d'un territoire.

    Lève sur un territoire inconnu plutôt que de supposer le Lambert 93. C'est
    exactement l'hypothèse qui rend faux le champ de position publié par
    l'ADEME : traiter la Nouvelle-Calédonie comme la Bourgogne place ses
    diagnostics à des milliers de kilomètres, sans la moindre erreur.
    """
    if department in OVERSEAS_SRID:
        return OVERSEAS_SRID[department]
    if department in METROPOLITAN:
        return METRIC_SRID
    raise ValueError(
        f"territoire « {department} » inconnu : son système de coordonnées n'est pas "
        "renseigné. L'ajouter à OVERSEAS_SRID après avoir vérifié qu'une coordonnée "
        "réelle y retombe bien, plutôt que de supposer le Lambert 93."
    )


def prepare(
    rows: list[dict],
    department: str,
    destination: Path,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Transforme des lignes d'API ADEME en un Parquet prepare."""
    if not rows:
        raise ValueError(f"aucun diagnostic a preparer pour le departement {department}")

    con = connection or duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    destination.parent.mkdir(parents=True, exist_ok=True)

    srid = srid_for(department)
    con.register("raw_rows", _as_relation(con, rows))

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW diagnostics AS
        SELECT
            numero_dpe,
            numero_dpe_remplace,
            CAST(date_etablissement_dpe AS DATE)          AS date_dpe,
            CAST(date_fin_validite_dpe AS DATE)           AS date_fin_validite,
            upper(etiquette_dpe)                          AS classe_dpe,
            upper(etiquette_ges)                          AS classe_ges,
            CAST(conso_5_usages_par_m2_ep AS DOUBLE)      AS conso_kwh_m2,
            CAST(emission_ges_5_usages_par_m2 AS DOUBLE)  AS ges_kg_m2,
            CAST(surface_habitable_logement AS DOUBLE)    AS surface_habitable,
            type_batiment,
            CAST(annee_construction AS SMALLINT)          AS annee_construction,
            periode_construction,
            adresse_ban                                   AS adresse,
            code_postal_ban                               AS code_postal,
            code_insee_ban                                AS code_commune,
            code_departement_ban                          AS code_departement,
            identifiant_ban                               AS ban_id,
            CAST(score_ban AS DOUBLE)                     AS score_ban,
            statut_geocodage,
            -- always_xy : sans lui, EPSG:4326 suit l'ordre d'axe officiel
            -- (latitude, longitude) et les deux valeurs sortent inversees.
            CASE WHEN coordonnee_cartographique_x_ban IS NOT NULL THEN
                ST_X(ST_Transform(
                    ST_Point(CAST(coordonnee_cartographique_x_ban AS DOUBLE),
                             CAST(coordonnee_cartographique_y_ban AS DOUBLE)),
                    'EPSG:{srid}', 'EPSG:4326', always_xy := true))
            END AS longitude,
            CASE WHEN coordonnee_cartographique_y_ban IS NOT NULL THEN
                ST_Y(ST_Transform(
                    ST_Point(CAST(coordonnee_cartographique_x_ban AS DOUBLE),
                             CAST(coordonnee_cartographique_y_ban AS DOUBLE)),
                    'EPSG:{srid}', 'EPSG:4326', always_xy := true))
            END AS latitude
        FROM raw_rows
        WHERE etiquette_dpe IS NOT NULL
    """)

    con.execute(f"""
        COPY (
            SELECT
                d.* EXCLUDE (numero_dpe_remplace),
                -- Un diagnostic cite par un autre comme remplace n'est plus la
                -- version courante du logement.
                d.numero_dpe IN (SELECT numero_dpe_remplace FROM diagnostics
                                 WHERE numero_dpe_remplace IS NOT NULL) AS est_remplace,
                CASE
                    WHEN d.longitude IS NULL                THEN 'sans_position'
                    WHEN d.score_ban IS NULL                THEN 'score_inconnu'
                    WHEN d.score_ban < {MIN_BAN_SCORE}      THEN 'position_incertaine'
                    ELSE 'ok'
                END AS qualite_position
            FROM diagnostics d
            ORDER BY d.date_dpe, d.numero_dpe
        ) TO '{destination}' (FORMAT parquet, COMPRESSION zstd)
    """)

    return _report(con, destination, len(rows), srid)


def _as_relation(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> duckdb.DuckDBPyRelation:
    """Materialise les lignes d'API, toutes colonnes attendues presentes.

    L'API omet les champs vides : sans normalisation, une colonne absente de la
    premiere page ferait echouer la requete plus loin.
    """
    # L'API renvoie les nombres en nombres et les absences en null. On ramene
    # tout au texte : les conversions de type se font une seule fois, en SQL, ou
    # elles sont visibles et testables.
    normalised = [
        {c: (None if row.get(c) is None else str(row[c])) for c in COLUMNS}
        for row in rows
    ]
    return con.from_arrow(_to_arrow(normalised))


def _to_arrow(rows: list[dict]):
    import pyarrow as pa

    return pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in COLUMNS]))


def _report(con: duckdb.DuckDBPyConnection, destination: Path, source_rows: int, srid: int) -> dict:
    row = con.execute(f"""
        SELECT
            count(*)                                      AS diagnostics,
            count(*) FILTER (NOT est_remplace)            AS courants,
            count(*) FILTER (qualite_position = 'ok')     AS bien_positionnes,
            min(date_dpe)                                 AS debut,
            max(date_dpe)                                 AS fin,
            min(latitude)                                 AS lat_min,
            max(latitude)                                 AS lat_max
        FROM read_parquet('{destination}')
    """).fetchone()

    total, courants, positionnes, debut, fin, lat_min, lat_max = row
    return {
        "lignes_source": source_rows,
        "diagnostics": total,
        "courants": courants,
        "remplaces": total - courants,
        "srid_source": srid,
        "part_bien_positionnee": round(positionnes / total * 100, 1) if total else None,
        "periode": [str(debut), str(fin)],
        "latitude_min_max": [lat_min, lat_max],
    }
