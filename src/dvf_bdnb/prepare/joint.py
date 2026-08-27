"""Le jeu joint : une ligne par vente, enrichie du bâtiment et du diagnostic.

C'est le seul des quatre jeux qui n'existe nulle part ailleurs. Et il est peu
coûteux à produire, parce que le lien est **relationnel, pas spatial** : la BDNB
publie `rel_batiment_groupe_parcelle.parcelle_id`, identifiant cadastral sur
14 caractères qui est exactement la clé `id_parcelle` de DVF.

    DVF.id_parcelle → BDNB parcelle → bâtiment → DPE

Trois règles qui décident de la qualité du résultat :

1. **Une vente reste une vente.** Toutes les jointures sont à gauche et
   dédoublonnées : un bien qui touche plusieurs bâtiments ou plusieurs
   diagnostics ne doit pas se démultiplier. Une vente non appariée est conservée,
   pas écartée.

2. **Le bâtiment décrit l'immeuble, pas le logement.** Vendre un appartement dans
   un immeuble de 1900 donne « année de construction 1900 » — ce qui est vrai de
   l'immeuble, et ne dit rien de l'état de l'appartement. La colonne
   `precision_bati` le rappelle sur chaque ligne.

3. **Le diagnostic doit être contemporain de la vente.** Un DPE établi cinq ans
   après ne décrit pas le bien tel qu'il a été acheté : entre-temps il a pu être
   rénové. On borne l'écart, et on retient le diagnostic le plus proche.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Écart maximal, en années, entre le diagnostic et la vente.
MAX_ANNEES_ECART = 3


def prepare(
    dvf_parquet: Path,
    destination: Path,
    *,
    bdnb_parquet: Path | None = None,
    dpe_parquet: Path | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Joint une vente à son bâtiment et à son diagnostic.

    `bdnb_parquet` est le pont : sans lui, le DPE reste inatteignable, faute de
    clé commune entre une vente et un diagnostic. Le jeu est alors produit quand
    même, avec ses colonnes bâti et énergie à vide — et le rapport le dit.
    """
    con = connection or duckdb.connect()
    destination.parent.mkdir(parents=True, exist_ok=True)

    dvf = _quote(dvf_parquet)
    avec_bdnb = bdnb_parquet is not None and bdnb_parquet.exists()
    avec_dpe = avec_bdnb and dpe_parquet is not None and dpe_parquet.exists()

    if avec_bdnb:
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW bati AS
            SELECT parcelle_id, batiment_groupe_id, identifiant_dpe,
                   annee_construction, materiau_mur, materiau_toit, nb_niveaux,
                   nb_logements, nb_logements_vacants, nb_logements_sociaux,
                   usage_principal, hauteur_moyenne,
                   classe_dpe AS classe_dpe_bdnb
            FROM read_parquet({_quote(bdnb_parquet)})
            WHERE parcelle_id IS NOT NULL
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW bati AS
            SELECT NULL::VARCHAR AS parcelle_id, NULL::VARCHAR AS batiment_groupe_id,
                   NULL::VARCHAR AS identifiant_dpe, NULL::SMALLINT AS annee_construction,
                   NULL::VARCHAR AS materiau_mur, NULL::VARCHAR AS materiau_toit,
                   NULL::SMALLINT AS nb_niveaux, NULL::INTEGER AS nb_logements,
                   NULL::INTEGER AS nb_logements_vacants, NULL::INTEGER AS nb_logements_sociaux,
                   NULL::VARCHAR AS usage_principal, NULL::DOUBLE AS hauteur_moyenne,
                   NULL::VARCHAR AS classe_dpe_bdnb
            WHERE false
        """)

    if avec_dpe:
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW diagnostics AS
            SELECT numero_dpe, date_dpe, classe_dpe, classe_ges, conso_kwh_m2,
                   surface_habitable, est_remplace
            FROM read_parquet({_quote(dpe_parquet)})
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW diagnostics AS
            SELECT NULL::VARCHAR AS numero_dpe, NULL::DATE AS date_dpe,
                   NULL::VARCHAR AS classe_dpe, NULL::VARCHAR AS classe_ges,
                   NULL::DOUBLE AS conso_kwh_m2, NULL::DOUBLE AS surface_habitable,
                   NULL::BOOLEAN AS est_remplace
            WHERE false
        """)

    con.execute(f"""
        COPY (
            -- DISTINCT ON : une vente reste une ligne, quel que soit le nombre de
            -- bâtiments sur sa parcelle ou de diagnostics sur son bâtiment.
            SELECT DISTINCT ON (v.cle_vente)
                v.*,
                b.batiment_groupe_id,
                b.annee_construction,
                b.materiau_mur,
                b.materiau_toit,
                b.nb_niveaux,
                b.nb_logements,
                b.nb_logements_vacants,
                b.nb_logements_sociaux,
                b.usage_principal,
                b.hauteur_moyenne,
                -- Le diagnostic de l'ADEME prime : il est republié chaque semaine,
                -- là où l'instantané embarqué par la BDNB l'est trois fois par an.
                COALESCE(d.classe_dpe, b.classe_dpe_bdnb)      AS classe_dpe,
                d.classe_ges,
                d.conso_kwh_m2,
                d.date_dpe,
                CASE
                    WHEN d.classe_dpe IS NOT NULL       THEN 'ademe'
                    WHEN b.classe_dpe_bdnb IS NOT NULL  THEN 'bdnb'
                END                                            AS source_dpe,
                CASE WHEN b.batiment_groupe_id IS NOT NULL
                     THEN 'groupe de batiments : decrit l''immeuble, pas le logement'
                END                                            AS precision_bati
            FROM read_parquet({dvf}) v
            LEFT JOIN bati b ON b.parcelle_id = v.id_parcelle
            LEFT JOIN diagnostics d
                   ON d.numero_dpe = b.identifiant_dpe
                  AND d.est_remplace IS NOT TRUE
                  AND abs(date_diff('year', d.date_dpe, v.date_mutation)) <= {MAX_ANNEES_ECART}
            ORDER BY v.cle_vente,
                     -- À égalité, le diagnostic le plus proche de la vente.
                     abs(date_diff('day', d.date_dpe, v.date_mutation)) NULLS LAST
        ) TO '{destination}' (FORMAT parquet, COMPRESSION zstd)
    """)

    return _report(con, destination, avec_bdnb=avec_bdnb, avec_dpe=avec_dpe)


def _quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _report(con: duckdb.DuckDBPyConnection, destination: Path, *, avec_bdnb: bool, avec_dpe: bool) -> dict:
    row = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (batiment_groupe_id IS NOT NULL),
               count(*) FILTER (classe_dpe IS NOT NULL),
               count(*) FILTER (source_dpe = 'ademe'),
               count(*) FILTER (annee_construction IS NOT NULL)
        FROM read_parquet({_quote(destination)})
    """).fetchone()

    ventes, avec_bat, avec_classe, depuis_ademe, avec_annee = row
    part = lambda n: round(n / ventes * 100, 1) if ventes else None  # noqa: E731

    manquantes = []
    if not avec_bdnb:
        manquantes.append("bdnb")
    if not avec_dpe:
        manquantes.append("dpe")

    return {
        "ventes": ventes,
        "part_avec_batiment": part(avec_bat),
        "part_avec_annee_construction": part(avec_annee),
        "part_avec_classe_energie": part(avec_classe),
        "part_energie_depuis_ademe": part(depuis_ademe),
        "sources_absentes": manquantes,
        "avertissement": (
            "Sans la BDNB, aucune vente ne peut atteindre un diagnostic : c'est elle "
            "qui porte la cle entre une parcelle et un batiment."
            if not avec_bdnb else None
        ),
    }
