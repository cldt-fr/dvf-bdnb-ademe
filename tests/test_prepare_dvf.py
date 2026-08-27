"""Comportements de la preparation DVF qu'on ne veut jamais voir regresser.

Le jeu d'essai tient six lignes et couvre les cas qui font qu'un DVF brut
mal exploite donne des chiffres faux.
"""

from pathlib import Path

import duckdb
import pytest

from dvf_bdnb.prepare import dvf

FIXTURE = Path(__file__).parent / "fixtures" / "geo-dvf-extrait.csv"


@pytest.fixture
def prepared(tmp_path: Path) -> Path:
    destination = tmp_path / "dept-48.parquet"
    dvf.prepare([FIXTURE], destination)
    return destination


def rows(path: Path) -> list[dict]:
    con = duckdb.connect()
    result = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY id_mutation")
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, r, strict=True)) for r in result.fetchall()]


def test_une_vente_multi_parcelles_ne_compte_qu_une_fois(prepared: Path) -> None:
    """Le piege central de DVF.

    La mutation 2024-1 porte sur deux parcelles et occupe deux lignes, mais
    `valeur_fonciere` y repete le prix de la vente ENTIERE. Sans regroupement,
    on compte deux ventes et on additionne 600 000 EUR pour un bien vendu
    300 000.
    """
    mutations = rows(prepared)
    assert len([m for m in mutations if m["id_mutation"] == "2024-1"]) == 1

    fusionnee = next(m for m in mutations if m["id_mutation"] == "2024-1")
    assert fusionnee["valeur_fonciere"] == 300000
    assert fusionnee["surface_bati"] == 150  # 100 + 50, les surfaces s'additionnent
    assert fusionnee["nb_parcelles"] == 2
    assert fusionnee["nb_lignes"] == 2


def test_les_echanges_sont_ecartes(prepared: Path) -> None:
    """Un echange n'est pas un prix negocie : il n'a pas sa place dans une mediane."""
    assert not [m for m in rows(prepared) if m["id_mutation"] == "2024-4"]


def test_une_vente_de_terrain_est_conservee_mais_marquee(prepared: Path) -> None:
    """On ne supprime pas : on signale. C'est au consommateur de trancher."""
    terrain = next(m for m in rows(prepared) if m["id_mutation"] == "2024-2")
    assert terrain["qualite_prix_m2"] == "sans_surface"
    assert terrain["prix_m2"] is None
    assert terrain["surface_terrain"] == 4000


def test_un_prix_au_m2_aberrant_est_signale_pas_supprime(prepared: Path) -> None:
    """90 000 EUR/m2 trahit une mutation mal decoupee, pas un bien de luxe."""
    aberrante = next(m for m in rows(prepared) if m["id_mutation"] == "2024-5")
    assert aberrante["qualite_prix_m2"] == "prix_m2_haut"
    assert aberrante["prix_m2"] == 90000


def test_une_vente_ordinaire_est_exploitable(prepared: Path) -> None:
    appartement = next(m for m in rows(prepared) if m["id_mutation"] == "2024-3")
    assert appartement["qualite_prix_m2"] == "ok"
    assert appartement["prix_m2"] == pytest.approx(4166.67, abs=0.01)
    assert appartement["type_local"] == "Appartement"


def test_les_codes_restent_du_texte(prepared: Path) -> None:
    """Le code commune corse 2A004 interdit de traiter les codes en entiers."""
    commune = rows(prepared)[0]["code_commune"]
    assert isinstance(commune, str)


def test_le_rapport_expose_le_dedoublonnage(tmp_path: Path) -> None:
    """Le rapport alimente les controles bloquants de la publication."""
    report = dvf.prepare([FIXTURE], tmp_path / "out.parquet")
    # Le rapport doit distinguer trois nombres qu'on confond facilement :
    # ce que contient le fichier, ce que le filtrage garde, ce que le
    # regroupement produit.
    assert report["lignes_source"] == 6      # tout le fichier
    assert report["lignes_retenues"] == 5    # l'echange est ecarte
    assert report["lignes_ecartees"] == 1
    assert report["mutations"] == 4          # les deux parcelles de 2024-1 fusionnent
    assert report["part_geolocalisee"] == 100.0
