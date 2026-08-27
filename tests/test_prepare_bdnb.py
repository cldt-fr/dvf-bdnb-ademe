"""Comportements de la préparation BDNB.

Le test central porte sur l'arbitrage parcelle → bâtiment : sans lui, une vente
serait comptée autant de fois qu'il y a de constructions sur son terrain.
"""

import csv
from pathlib import Path

import duckdb
import pytest

from dvf_bdnb.prepare import bdnb


def ecrire(repertoire: Path, table: str, colonnes: list[str], lignes: list[list]) -> None:
    repertoire.mkdir(parents=True, exist_ok=True)
    with (repertoire / f"{table}.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(colonnes)
        w.writerows(lignes)


@pytest.fixture
def jeu(tmp_path: Path) -> Path:
    """Deux bâtiments sur la même parcelle, plus un isolé sur une autre."""
    csv_dir = tmp_path / "csv"
    ecrire(csv_dir, "batiment_groupe",
           ["batiment_groupe_id", "code_commune_insee", "code_departement_insee",
            "s_geom_groupe", "geom_groupe"],
           [["BG1", "33063", "33", "120", "POLYGON((0 0,1 0,1 1,0 1,0 0))"],
            ["BG2", "33063", "33", "40", "POLYGON((2 0,3 0,3 1,2 1,2 0))"],
            ["BG3", "48095", "48", "80", "POLYGON((4 0,5 0,5 1,4 1,4 0))"]])
    ecrire(csv_dir, "batiment_groupe_ffo_bat",
           ["batiment_groupe_id", "annee_construction", "mat_mur_txt", "mat_toit_txt",
            "nb_niveau", "nb_log", "nb_log_vac", "nb_log_residence_principale",
            "nb_log_residence_secondaire", "nb_log_soc", "s_log", "usage_principal"],
           [["BG1", "1905", "pierre", "tuile", "3", "12", "1", "10", "1", "0", "800", "résidentiel"],
            ["BG2", "1998", "beton", "tuile", "1", "1", "0", "1", "0", "0", "60", "annexe"],
            ["BG3", "1972", "brique", "ardoise", "2", "4", "0", "4", "0", "0", "300", "résidentiel"]])
    ecrire(csv_dir, "rel_batiment_groupe_parcelle",
           ["batiment_groupe_id", "parcelle_id", "code_departement_insee", "parcelle_principale"],
           # BG1 et BG2 partagent la parcelle ; BG2 est marqué principal, mais
           # BG1 abrite douze logements contre un.
           [["BG1", "33063000AB0001", "33", "f"],
            ["BG2", "33063000AB0001", "33", "t"],
            ["BG3", "48095000AC0002", "48", "t"]])
    return csv_dir


def rows(path: Path) -> dict[str, dict]:
    con = duckdb.connect()
    result = con.execute(f"SELECT * EXCLUDE (geom) FROM read_parquet('{path}')")
    columns = [d[0] for d in result.description]
    return {r[0]: dict(zip(columns, r, strict=True)) for r in result.fetchall()}


def test_une_parcelle_ne_designe_qu_un_batiment(jeu: Path, tmp_path: Path) -> None:
    """Sans arbitrage, une vente serait comptée deux fois sur cette parcelle."""
    destination = tmp_path / "bdnb.parquet"
    bdnb.prepare(jeu, destination)

    produits = rows(destination)
    rattaches = [b for b in produits.values() if b["parcelle_id"] == "33063000AB0001"]
    assert len(rattaches) == 1


def test_la_parcelle_principale_l_emporte_sur_le_nombre_de_logements(jeu: Path, tmp_path: Path) -> None:
    """L'ordre des critères compte : le marquage du CSTB prime sur notre heuristique."""
    destination = tmp_path / "bdnb.parquet"
    bdnb.prepare(jeu, destination)

    produits = rows(destination)
    assert produits["BG2"]["parcelle_id"] == "33063000AB0001"
    assert produits["BG1"]["parcelle_id"] is None


def test_le_filtre_par_departement_s_applique(jeu: Path, tmp_path: Path) -> None:
    destination = tmp_path / "bdnb.parquet"
    bdnb.prepare(jeu, destination, departments=["48"])
    assert set(rows(destination)) == {"BG3"}


def test_les_tables_absentes_sont_signalees_pas_fatales(jeu: Path, tmp_path: Path) -> None:
    """Le DPE représentatif et la BD TOPO sont des enrichissements.

    Leur absence ne doit pas empêcher de produire le jeu : elle doit être dite.
    """
    destination = tmp_path / "bdnb.parquet"
    rapport = bdnb.prepare(jeu, destination)

    assert "batiment_groupe_dpe_representatif_logement" in rapport["tables_absentes"]
    assert rapport["batiments"] == 3
    assert rows(destination)["BG1"]["classe_dpe"] is None


def test_une_table_indispensable_absente_est_une_erreur_claire(tmp_path: Path) -> None:
    vide = tmp_path / "csv"
    vide.mkdir()
    with pytest.raises(FileNotFoundError, match="batiment_groupe"):
        bdnb.prepare(vide, tmp_path / "out.parquet")


def test_le_rapport_expose_la_couverture(jeu: Path, tmp_path: Path) -> None:
    """Ces taux alimentent les contrôles bloquants de la publication."""
    rapport = bdnb.prepare(jeu, tmp_path / "bdnb.parquet")

    assert rapport["batiments"] == 3
    assert rapport["logements"] == 17           # 12 + 1 + 4
    assert rapport["part_avec_annee"] == 100.0
    assert rapport["part_avec_geometrie"] == 100.0
    # Deux bâtiments sur trois portent une parcelle : BG1 a cédé la sienne à BG2.
    assert rapport["part_rattachee_parcelle"] == pytest.approx(66.7, abs=0.1)


# --- Dialecte de l'export ----------------------------------------------------
#
# Le separateur etait code a la virgule. L'export BDNB utilise le point-virgule :
# verifie sur le millesime 2026-02-a, l'en-tete de batiment_groupe.csv contient
# zero virgule et huit points-virgules. DuckDB aurait rendu une colonne unique
# contenant la ligne entiere, et l'erreur suivante aurait parle d'une colonne
# manquante — envoyant chercher du cote du schema plutot que du dialecte.


def test_le_separateur_est_lu_dans_l_entete(tmp_path: Path) -> None:
    fichier = tmp_path / "batiment_groupe.csv"
    fichier.write_text(
        "geom_groupe;batiment_groupe_id;code_departement_insee\n"
        '"MULTIPOLYGON (((848242.5 6563290.1, 848243.0 6563291.0)))";bg1;48\n',
        encoding="utf-8",
    )
    assert bdnb._delimiter(fichier) == ";"


def test_la_geometrie_pleine_de_virgules_ne_trompe_pas_la_detection(tmp_path: Path) -> None:
    """C'est le piege : les DONNEES contiennent bien plus de virgules que de
    points-virgules. Seul l'en-tete est fiable."""
    fichier = tmp_path / "batiment_groupe.csv"
    virgules = ", ".join(f"{i}.0 {i}.5" for i in range(50))
    fichier.write_text(
        "geom_groupe;batiment_groupe_id\n" f'"MULTIPOLYGON ((({virgules})))";bg1\n',
        encoding="utf-8",
    )
    assert fichier.read_text(encoding="utf-8").count(",") > 40
    assert bdnb._delimiter(fichier) == ";"


def test_un_entete_sans_separateur_est_denonce(tmp_path: Path) -> None:
    fichier = tmp_path / "batiment_groupe.csv"
    fichier.write_text("colonneunique\nvaleur\n", encoding="utf-8")

    with pytest.raises(ValueError, match="aucun separateur reconnu"):
        bdnb._delimiter(fichier)


def test_un_fichier_en_point_virgule_est_lu_correctement(tmp_path: Path) -> None:
    """Bout en bout : c'est ce que produit reellement l'archive BDNB."""
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "batiment_groupe.csv").write_text(
        "batiment_groupe_id;code_commune_insee;code_departement_insee;s_geom_groupe\n"
        "bg1;48095;48;120.5\n"
        "bg2;48095;48;80.0\n",
        encoding="utf-8",
    )

    con = duckdb.connect()
    trouves = bdnb._register(con, csv_dir)

    assert "groupe" in trouves
    colonnes = [c[0] for c in con.execute("DESCRIBE groupe").fetchall()]
    assert "batiment_groupe_id" in colonnes
    assert con.execute("SELECT count(*) FROM groupe").fetchone()[0] == 2
