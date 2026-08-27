"""Préparation du jeu BDNB (CSTB).

Ce qui est fait ici, et pourquoi :

- **90 tables ramenées à une.** Le dump porte tout le modèle CSTB. On projette
  une ligne par groupe de bâtiments, avec ce dont on se sert réellement :
  époque de construction, matériaux, hauteur, niveaux, nombre et occupation des
  logements, usage, classe énergétique représentative.
- **Rattachement à la parcelle cadastrale conservé.** C'est ce qui rend la
  jonction avec DVF possible sans le moindre calcul spatial :
  `rel_batiment_groupe_parcelle.parcelle_id` porte l'identifiant cadastral sur
  14 caractères, exactement la clé des parcelles DVF.
- **Une parcelle, un bâtiment.** Une parcelle peut porter plusieurs
  constructions (cour, dépendances, immeubles multiples). Sans arbitrage, une
  vente serait comptée autant de fois qu'il y a de bâtiments sur son terrain.
  On retient la parcelle principale d'abord, puis le bâtiment le plus habité.
- **Géométrie ramenée en WGS 84**, le format d'origine étant détecté et non
  supposé.

Le nom du schéma et celui de la table des DPE représentatifs changent d'un
millésime à l'autre : rien de tout cela n'est codé en dur, ni ici ni ailleurs.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Lambert 93 : système de référence des géométries BDNB en métropole.
DEFAULT_SRID = 2154

# Tables extraites de l'archive. Le reste du modèle CSTB ne nous sert pas.
TABLES = {
    "groupe": "batiment_groupe",
    "ffo": "batiment_groupe_ffo_bat",
    "topo": "batiment_groupe_bdtopo_bat",
    "dpe": "batiment_groupe_dpe_representatif_logement",
    "parcelle": "rel_batiment_groupe_parcelle",
}


def prepare(
    csv_dir: Path,
    destination: Path,
    *,
    departments: list[str] | None = None,
    srid: int = DEFAULT_SRID,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Projette les CSV BDNB en un Parquet préparé."""
    con = connection or duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    destination.parent.mkdir(parents=True, exist_ok=True)

    available = _register(con, csv_dir)
    _assert_minimum(available)

    dept_filter = ""
    if departments:
        codes = ", ".join("'" + d.replace("'", "''") + "'" for d in departments)
        dept_filter = f"WHERE g.code_departement_insee IN ({codes})"

    geom_expr = _geometry_expression(con, srid) if "groupe" in available else "NULL"
    dpe_columns, dpe_join = _optional_dpe(available)
    topo_column, topo_join = _optional_topo(available)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW batiments AS
        SELECT
            g.batiment_groupe_id,
            g.code_commune_insee                              AS code_commune,
            g.code_departement_insee                           AS code_departement,
            TRY_CAST(g.s_geom_groupe AS INTEGER)               AS emprise_sol,
            TRY_CAST(f.annee_construction AS SMALLINT)         AS annee_construction,
            f.mat_mur_txt                                      AS materiau_mur,
            f.mat_toit_txt                                     AS materiau_toit,
            TRY_CAST(f.nb_niveau AS SMALLINT)                  AS nb_niveaux,
            TRY_CAST(f.nb_log AS INTEGER)                      AS nb_logements,
            TRY_CAST(f.nb_log_vac AS INTEGER)                  AS nb_logements_vacants,
            TRY_CAST(f.nb_log_residence_principale AS INTEGER) AS nb_residences_principales,
            TRY_CAST(f.nb_log_residence_secondaire AS INTEGER) AS nb_residences_secondaires,
            TRY_CAST(f.nb_log_soc AS INTEGER)                  AS nb_logements_sociaux,
            TRY_CAST(f.s_log AS INTEGER)                       AS surface_logements,
            f.usage_principal                                  AS usage_principal,
            {topo_column}
            {dpe_columns}
            {geom_expr}                                        AS geom
        FROM groupe g
        LEFT JOIN ffo f ON f.batiment_groupe_id = g.batiment_groupe_id
        {topo_join}
        {dpe_join}
        {dept_filter}
    """)

    if "parcelle" in available:
        # DISTINCT ON : une parcelle peut porter plusieurs bâtiments, et sans
        # arbitrage une vente serait comptée autant de fois qu'il y a de
        # constructions sur son terrain.
        con.execute("""
            CREATE OR REPLACE TEMP VIEW parcelles AS
            SELECT DISTINCT ON (r.parcelle_id)
                r.parcelle_id,
                r.batiment_groupe_id
            FROM parcelle r
            JOIN batiments b ON b.batiment_groupe_id = r.batiment_groupe_id
            ORDER BY r.parcelle_id,
                     CASE WHEN lower(CAST(r.parcelle_principale AS VARCHAR)) IN ('t', 'true', '1')
                          THEN 0 ELSE 1 END,
                     b.nb_logements DESC NULLS LAST
        """)
        select = """
            SELECT b.*, p.parcelle_id
            FROM batiments b
            LEFT JOIN parcelles p ON p.batiment_groupe_id = b.batiment_groupe_id
        """
    else:
        select = "SELECT b.*, NULL AS parcelle_id FROM batiments b"

    con.execute(f"""
        COPY ({select} ORDER BY b.batiment_groupe_id)
        TO '{destination}' (FORMAT parquet, COMPRESSION zstd)
    """)

    return _report(con, destination, available)


def _register(con: duckdb.DuckDBPyConnection, csv_dir: Path) -> set[str]:
    """Déclare les CSV présents. Une table absente est signalée, pas fatale."""
    found: set[str] = set()
    for alias, table in TABLES.items():
        path = csv_dir / f"{table}.csv"
        if not path.exists():
            continue
        escaped = str(path).replace("'", "''")
        delim = _delimiter(path)
        # Le separateur est LU dans l'en-tete, jamais devine dans les donnees.
        #
        # Il etait code a la virgule : l'export BDNB utilise le point-virgule
        # (verifie sur le millesime 2026-02-a — zero virgule et huit
        # points-virgules dans l'en-tete de batiment_groupe.csv). DuckDB aurait
        # rendu une colonne unique contenant la ligne entiere.
        #
        # Deviner d'apres les DONNEES reste exclu : un champ entre guillemets
        # contenant une virgule suffit a faire deraper un sniffer, et la
        # geometrie WKT de la BDNB en est pleine. L'en-tete, lui, ne contient
        # que des noms de colonnes.
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW {alias} AS
            SELECT * FROM read_csv(
                '{escaped}',
                header = true, all_varchar = true,
                delim = '{delim}', quote = '"', escape = '"'
            )
        """)
        _assert_split(con, alias, path, delim)
        found.add(alias)
    return found


# Separateurs plausibles, du plus probable au moins probable pour cette source.
DELIMITERS = (";", ",", "\t", "|")


def _delimiter(path: Path) -> str:
    """Separateur du fichier, lu dans son en-tete.

    L'en-tete ne contient que des noms de colonnes : aucun guillemet, aucune
    virgule decorative. C'est la seule ligne du fichier ou compter les
    separateurs est sans risque.
    """
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        header = handle.readline()

    counts = {d: header.count(d) for d in DELIMITERS}
    best = max(counts, key=counts.get)

    if counts[best] == 0:
        raise ValueError(
            f"aucun separateur reconnu dans l'en-tete de {path.name} : "
            f"{header[:120]!r}. Le format de l'export BDNB a peut-etre change."
        )
    return "\\t" if best == "\t" else best


def _assert_split(con: duckdb.DuckDBPyConnection, alias: str, path: Path, delim: str) -> None:
    """Verifie que le fichier a bien ete decoupe.

    Une seule colonne lue signifie que le separateur ne correspond pas. Sans ce
    controle, l'erreur qui suit parle d'une colonne manquante et envoie chercher
    du cote du schema plutot que du dialecte.
    """
    colonnes = con.execute(f"DESCRIBE {alias}").fetchall()
    if len(colonnes) > 1:
        return

    raise ValueError(
        f"{path.name} n'a pas ete decoupe en colonnes avec le separateur "
        f"« {delim} » : une seule colonne lue. L'export a probablement change "
        "de dialecte."
    )


def _assert_minimum(available: set[str]) -> None:
    missing = [TABLES[a] for a in ("groupe", "ffo") if a not in available]
    if missing:
        raise FileNotFoundError(
            "tables BDNB indispensables absentes : " + ", ".join(missing) +
            ". Vérifier que l'archive a bien été extraite dans ce répertoire."
        )


def _geometry_expression(con: duckdb.DuckDBPyConnection, srid: int) -> str:
    """Détermine comment lire la géométrie plutôt que de le supposer.

    Selon l'export, `geom_groupe` arrive en WKT (`POLYGON((...))`) ou en WKB
    hexadécimal. Choisir au hasard produirait soit une erreur, soit — bien pire —
    une colonne entièrement nulle sans le moindre message.
    """
    sample = con.execute(
        "SELECT geom_groupe FROM groupe WHERE geom_groupe IS NOT NULL LIMIT 1"
    ).fetchone()

    if sample is None:
        return "NULL"

    value = str(sample[0]).strip()
    if value[:1].isalpha() and "(" in value:
        reader = "ST_GeomFromText(g.geom_groupe)"
    else:
        reader = "ST_GeomFromHEXWKB(g.geom_groupe)"

    # always_xy : sans lui, EPSG:4326 suit l'ordre d'axe officiel et les
    # coordonnées sortent interverties.
    return (
        f"ST_AsWKB(ST_Transform({reader}, 'EPSG:{srid}', 'EPSG:4326', always_xy := true))"
    )


def _optional_dpe(available: set[str]) -> tuple[str, str]:
    if "dpe" not in available:
        return (
            "NULL::VARCHAR AS identifiant_dpe, NULL::VARCHAR AS classe_dpe, "
            "NULL::VARCHAR AS classe_ges, NULL::DOUBLE AS conso_kwh_m2,",
            "",
        )
    return (
        """d.identifiant_dpe                              AS identifiant_dpe,
           upper(d.classe_bilan_dpe)                       AS classe_dpe,
           upper(d.classe_emission_ges)                    AS classe_ges,
           TRY_CAST(d.conso_5_usages_ep_m2 AS DOUBLE)      AS conso_kwh_m2,""",
        "LEFT JOIN dpe d ON d.batiment_groupe_id = g.batiment_groupe_id",
    )


def _optional_topo(available: set[str]) -> tuple[str, str]:
    if "topo" not in available:
        return "NULL::DOUBLE AS hauteur_moyenne,", ""
    return (
        "TRY_CAST(t.hauteur_mean AS DOUBLE) AS hauteur_moyenne,",
        "LEFT JOIN topo t ON t.batiment_groupe_id = g.batiment_groupe_id",
    )


def _report(con: duckdb.DuckDBPyConnection, destination: Path, available: set[str]) -> dict:
    row = con.execute(f"""
        SELECT
            count(*)                                        AS batiments,
            count(*) FILTER (parcelle_id IS NOT NULL)       AS rattaches,
            count(*) FILTER (classe_dpe IS NOT NULL)        AS avec_dpe,
            count(*) FILTER (annee_construction IS NOT NULL) AS avec_annee,
            count(*) FILTER (geom IS NOT NULL)              AS avec_geometrie,
            sum(COALESCE(nb_logements, 0))                  AS logements
        FROM read_parquet('{destination}')
    """).fetchone()

    total, rattaches, avec_dpe, avec_annee, avec_geom, logements = row
    pct = lambda n: round(n / total * 100, 1) if total else None  # noqa: E731

    return {
        "batiments": total,
        "logements": int(logements or 0),
        "part_rattachee_parcelle": pct(rattaches),
        "part_avec_dpe": pct(avec_dpe),
        "part_avec_annee": pct(avec_annee),
        "part_avec_geometrie": pct(avec_geom),
        "tables_absentes": sorted(TABLES[a] for a in TABLES if a not in available),
    }
