"""Comportements de la preparation DPE.

Le test central porte sur la reprojection : c'est le seul endroit ou notre jeu
est demontrable-ment plus juste que la source officielle.
"""

from pathlib import Path

import duckdb
import pytest

from dvf_bdnb.prepare import dpe


def ligne(**kw) -> dict:
    base = {c: None for c in dpe.COLUMNS}
    base.update(kw)
    return base


def rows(path: Path) -> list[dict]:
    con = duckdb.connect()
    result = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY numero_dpe")
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, r, strict=True)) for r in result.fetchall()]


def test_le_systeme_source_depend_du_territoire() -> None:
    """L'ADEME publie en systeme metrique LOCAL, pas en Lambert 93 partout."""
    assert dpe.srid_for("33") == 2154   # Gironde, Lambert 93
    assert dpe.srid_for("2A") == 2154   # Corse, Lambert 93
    assert dpe.srid_for("971") == 5490  # Guadeloupe, UTM 20N
    assert dpe.srid_for("973") == 2972  # Guyane, UTM 22N
    assert dpe.srid_for("974") == 2975  # La Reunion, UTM 40S
    assert dpe.srid_for("976") == 4471  # Mayotte, UTM 38S


@pytest.mark.parametrize(
    ("departement", "x", "y", "lat", "lon"),
    [
        ("33", 415022, 6422103, 44.84, -0.61),      # Bordeaux
        ("974", 337000, 7676000, -21.01, 55.43),    # La Reunion
        ("971", 653431, 1774592, 16.05, -61.57),    # Guadeloupe
    ],
)
def test_les_diagnostics_atterrissent_dans_leur_territoire(
    tmp_path: Path, departement: str, x: float, y: float, lat: float, lon: float
) -> None:
    """Le champ `_geopoint` publie par l'ADEME est faux en outre-mer.

    Il traite toutes les coordonnees comme du Lambert 93 : Saint-Paul de La
    Reunion y tombe en mer du Nord, Capesterre-Belle-Eau dans le golfe de
    Guinee. On reprojette donc nous-memes, avec le systeme du territoire.
    """
    source = ligne(
        numero_dpe="X1", etiquette_dpe="D", date_etablissement_dpe="2024-01-01",
        code_departement_ban=departement, score_ban="0.9",
        coordonnee_cartographique_x_ban=str(x), coordonnee_cartographique_y_ban=str(y),
    )
    destination = tmp_path / f"dept-{departement}.parquet"
    dpe.prepare([source], departement, destination)

    produced = rows(destination)[0]
    assert produced["latitude"] == pytest.approx(lat, abs=0.02)
    assert produced["longitude"] == pytest.approx(lon, abs=0.02)


def test_latitude_et_longitude_ne_sont_pas_inversees(tmp_path: Path) -> None:
    """Piege d'axe : avec EPSG:4326, PROJ suit l'ordre officiel (lat, lon).

    Sans `always_xy`, ST_X renvoie la latitude et les deux valeurs sortent
    interverties — une erreur muette qui place tout le jeu de travers.
    """
    source = ligne(
        numero_dpe="X1", etiquette_dpe="C", date_etablissement_dpe="2024-01-01",
        code_departement_ban="33", score_ban="0.9",
        coordonnee_cartographique_x_ban="415022", coordonnee_cartographique_y_ban="6422103",
    )
    produced = rows(_prepared(tmp_path, [source], "33"))[0]
    # En France metropolitaine la latitude est toujours superieure a la
    # longitude : une inversion se verrait immediatement.
    assert produced["latitude"] > produced["longitude"]
    assert 41 < produced["latitude"] < 52


def test_un_diagnostic_remplace_est_signale(tmp_path: Path) -> None:
    """Sans ce marquage, un logement revise compte plusieurs fois."""
    sources = [
        ligne(numero_dpe="ANCIEN", etiquette_dpe="E", date_etablissement_dpe="2022-01-01",
              code_departement_ban="33", score_ban="0.9"),
        ligne(numero_dpe="NOUVEAU", numero_dpe_remplace="ANCIEN", etiquette_dpe="C",
              date_etablissement_dpe="2024-01-01", code_departement_ban="33", score_ban="0.9"),
    ]
    produced = {r["numero_dpe"]: r for r in rows(_prepared(tmp_path, sources, "33"))}
    assert produced["ANCIEN"]["est_remplace"] is True
    assert produced["NOUVEAU"]["est_remplace"] is False


def test_une_position_incertaine_est_marquee_pas_supprimee(tmp_path: Path) -> None:
    sources = [
        ligne(numero_dpe="FLOU", etiquette_dpe="D", date_etablissement_dpe="2024-01-01",
              code_departement_ban="33", score_ban="0.2",
              coordonnee_cartographique_x_ban="415022", coordonnee_cartographique_y_ban="6422103"),
        ligne(numero_dpe="SANS", etiquette_dpe="D", date_etablissement_dpe="2024-01-01",
              code_departement_ban="33", score_ban="0.9"),
    ]
    produced = {r["numero_dpe"]: r for r in rows(_prepared(tmp_path, sources, "33"))}
    assert produced["FLOU"]["qualite_position"] == "position_incertaine"
    assert produced["SANS"]["qualite_position"] == "sans_position"


def test_un_diagnostic_sans_classe_est_ecarte(tmp_path: Path) -> None:
    """Sans etiquette, le diagnostic n'a aucune valeur analytique."""
    sources = [
        ligne(numero_dpe="AVEC", etiquette_dpe="B", date_etablissement_dpe="2024-01-01",
              code_departement_ban="33"),
        ligne(numero_dpe="SANS_CLASSE", date_etablissement_dpe="2024-01-01",
              code_departement_ban="33"),
    ]
    produced = rows(_prepared(tmp_path, sources, "33"))
    assert [r["numero_dpe"] for r in produced] == ["AVEC"]


def _prepared(tmp_path: Path, sources: list[dict], departement: str) -> Path:
    destination = tmp_path / "out.parquet"
    dpe.prepare(sources, departement, destination)
    return destination
