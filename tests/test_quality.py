"""Contrôles qualité bloquants.

Le test central : le contrôle d'emprise aurait attrapé seul le bug de
coordonnées de l'ADEME, sans que personne ait à regarder une carte.
"""

from pathlib import Path

import duckdb
import pytest

from dvf_bdnb import quality
from dvf_bdnb.quality import Level


def jeu(tmp_path: Path, points: list[tuple[float, float]], nom: str = "d.parquet") -> Path:
    destination = tmp_path / nom
    con = duckdb.connect()
    if points:
        valeurs = ", ".join(f"({lat}, {lon})" for lat, lon in points)
        con.execute(f"""
            COPY (SELECT * FROM (VALUES {valeurs}) AS t(latitude, longitude))
            TO '{destination}' (FORMAT parquet)
        """)
    else:
        con.execute(f"""
            COPY (SELECT NULL::DOUBLE AS latitude, NULL::DOUBLE AS longitude WHERE false)
            TO '{destination}' (FORMAT parquet)
        """)
    return destination


def test_le_bug_de_coordonnees_de_lademe_serait_attrape(tmp_path: Path) -> None:
    """Le `_geopoint` publié par l'ADEME place Saint-Paul de La Réunion à la
    latitude 56, en mer du Nord, parce qu'il traite toutes les coordonnées
    comme du Lambert 93.

    Ce contrôle le voit sans qu'aucun humain ait à regarder une carte.
    """
    faux = jeu(tmp_path, [(56.006, -3.001), (55.9, -3.1), (56.1, -2.9)])
    rapport = quality.check_dataset(faux, "dpe", "974")

    assert not rapport.passed
    emprise = next(f for f in rapport.findings if f.check == "emprise")
    assert emprise.level is Level.FAIL
    assert "reprojection" in emprise.message


def test_des_coordonnees_justes_passent(tmp_path: Path) -> None:
    """Les mêmes diagnostics, correctement reprojetés."""
    juste = jeu(tmp_path, [(-21.01, 55.43), (-21.2, 55.5), (-20.9, 55.3)])
    rapport = quality.check_dataset(juste, "dpe", "974")

    assert rapport.passed
    assert next(f for f in rapport.findings if f.check == "emprise").level is Level.OK


def test_chaque_territoire_a_sa_propre_emprise(tmp_path: Path) -> None:
    """Un point antillais est juste en Guadeloupe et faux en métropole."""
    antilles = jeu(tmp_path, [(16.05, -61.57)])
    assert quality.check_dataset(antilles, "dpe", "971").passed
    assert not quality.check_dataset(antilles, "dpe", "33").passed


def test_quelques_points_egares_ne_bloquent_pas(tmp_path: Path) -> None:
    """Une saisie isolée n'est pas une reprojection ratée : on avertit."""
    points = [(44.8, -0.6)] * 2000 + [(0.0, 0.0)]
    rapport = quality.check_dataset(jeu(tmp_path, points), "dvf", "33")

    assert rapport.passed
    assert next(f for f in rapport.findings if f.check == "emprise").level is Level.WARN


def test_un_departement_hors_couverture_dvf_peut_etre_vide(tmp_path: Path) -> None:
    """Le Bas-Rhin relève du livre foncier : DVF n'y publie rien.

    Confondre cette absence avec une panne ferait bloquer une publication
    parfaitement valide, tous les mois, pour toujours.
    """
    rapport = quality.check_dataset(jeu(tmp_path, []), "dvf", "67")
    assert rapport.passed
    assert "hors couverture" in next(f for f in rapport.findings if f.check == "volume").message


def test_un_departement_couvert_et_vide_bloque(tmp_path: Path) -> None:
    rapport = quality.check_dataset(jeu(tmp_path, []), "dvf", "33")
    assert not rapport.passed


def test_une_chute_de_volume_bloque(tmp_path: Path) -> None:
    """Un jeu qui perd la moitié de ses lignes signale un problème amont."""
    rapport = quality.check_dataset(
        jeu(tmp_path, [(44.8, -0.6)] * 100), "dvf", "33",
        baseline={"lignes": 1000},
    )
    assert not rapport.passed
    regression = next(f for f in rapport.findings if f.check == "regression")
    assert regression.detail["precedent"] == 1000


def test_une_croissance_normale_passe(tmp_path: Path) -> None:
    rapport = quality.check_dataset(
        jeu(tmp_path, [(44.8, -0.6)] * 1100), "dvf", "33",
        baseline={"lignes": 1000},
    )
    assert rapport.passed


def test_un_fichier_absent_bloque(tmp_path: Path) -> None:
    rapport = quality.check_dataset(tmp_path / "fantome.parquet", "dvf", "33")
    assert not rapport.passed


def test_la_reference_se_lit_dans_le_manifeste_precedent() -> None:
    """Le manifeste publié est la seule mémoire qui ne peut pas diverger."""
    baseline = quality.baseline_from_manifest({
        "sources": {
            "dvf": {"fichiers": [
                {"fichier": "dept-33.parquet", "lignes": 120000},
                {"fichier": "dept-33.csv.gz"},
            ]}
        }
    })
    assert baseline == {"dvf/33": {"lignes": 120000}}
