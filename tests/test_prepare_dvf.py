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


def test_une_virgule_dans_une_adresse_ne_casse_pas_la_lecture(tmp_path: Path) -> None:
    """Cas réel, rencontré sur quatre départements du jeu complet.

    DuckDB déduit le dialecte du PREMIER fichier d'une liste. Si celui-ci n'a
    aucun champ entre guillemets, il conclut qu'il n'y en a pas — et le premier
    nom de voie contenant une virgule fait alors compter 41 colonnes au lieu de
    40, dans un autre fichier.

    On impose donc le dialecte au lieu de le laisser deviner.
    """
    avec_virgule = FIXTURE.parent / "geo-dvf-virgule-dans-adresse.csv"
    destination = tmp_path / "melange.parquet"

    # L'ordre compte : le fichier sans guillemets vient en premier, exactement
    # comme dans le cas réel.
    dvf.prepare([FIXTURE, avec_virgule], destination)

    produites = rows(destination)
    oradour = next(m for m in produites if m["nom_commune"] == "Oradour")
    assert oradour["valeur_fonciere"] == 10000
    assert oradour["surface_bati"] == 66


def test_une_vente_a_cheval_sur_deux_communes_reste_une_vente(tmp_path: Path) -> None:
    """Cas réel, 1,8 % des ventes de la Lozère.

    Une vente peut porter sur des parcelles situées dans des communes
    différentes. Grouper sur l'ensemble des colonnes la redécoupe en autant de
    lignes, dont chacune répète le prix TOTAL — exactement le défaut qu'on
    corrige chez DVF, reproduit à plus petite échelle.
    """
    import csv

    header = FIXTURE.read_text(encoding="utf-8").splitlines()[0].split(",")

    def ligne(**kw):
        r = {c: "" for c in header}
        r.update(kw)
        return [r[c] for c in header]

    source = tmp_path / "deux-communes.csv"
    with source.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows([
            ligne(id_mutation="2024-9", date_mutation="2024-05-01", nature_mutation="Vente",
                  valeur_fonciere="18000", code_commune="48040", nom_commune="Chastel-Nouvel",
                  code_departement="48", id_parcelle="48040000AA0001",
                  surface_reelle_bati="80", longitude="3.5", latitude="44.5"),
            ligne(id_mutation="2024-9", date_mutation="2024-05-01", nature_mutation="Vente",
                  valeur_fonciere="18000", code_commune="48082", nom_commune="Lachamp-Ribennes",
                  code_departement="48", id_parcelle="48082000AB0002",
                  surface_reelle_bati="20", longitude="3.6", latitude="44.6"),
        ])

    destination = tmp_path / "out.parquet"
    dvf.prepare([source], destination)
    produites = rows(destination)

    assert len(produites) == 1
    vente = produites[0]
    assert vente["valeur_fonciere"] == 18000        # et non 36 000
    assert vente["surface_bati"] == 100             # les surfaces, elles, s'additionnent
    assert vente["nb_communes"] == 2                # le fait est exposé, pas masqué
    # La localisation retenue est celle de la parcelle la plus bâtie.
    assert vente["nom_commune"] == "Chastel-Nouvel"


def _ecrire(chemin: Path, lignes: list[dict]) -> Path:
    """Ecrit un CSV geo-dvf minimal a l'emplacement demande."""
    import csv

    header = FIXTURE.read_text(encoding="utf-8").splitlines()[0].split(",")
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with chemin.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for l in lignes:
            r = {c: "" for c in header}
            r.update(l)
            w.writerow([r[c] for c in header])
    return chemin


VENTE = {
    "date_mutation": "2020-03-04", "nature_mutation": "Vente",
    "valeur_fonciere": "250000", "code_commune": "48095", "nom_commune": "Mende",
    "code_departement": "48", "id_parcelle": "48095000AB0012",
    "surface_reelle_bati": "90", "code_type_local": "1", "type_local": "Maison",
    "longitude": "3.5", "latitude": "44.5",
}


def test_la_cle_de_vente_ne_bouge_pas_quand_id_mutation_change(tmp_path: Path) -> None:
    """`id_mutation` est un numero de sequence reattribue a chaque publication.

    Constate en Lozere : les 9 457 lignes de 2021 portent un id different entre
    le millesime 2023 et celui de 2025, decale de 1 927, pour les memes ventes.
    Une cle calculee depuis le contenu doit, elle, rester identique.
    """
    a = dvf.prepare([_ecrire(tmp_path / "a" / "48.csv", [{**VENTE, "id_mutation": "2020-821256"}])],
                    tmp_path / "a.parquet")
    b = dvf.prepare([_ecrire(tmp_path / "b" / "48.csv", [{**VENTE, "id_mutation": "2020-823183"}])],
                    tmp_path / "b.parquet")
    assert a["mutations"] == b["mutations"] == 1

    cle_a = rows(tmp_path / "a.parquet")[0]["cle_vente"]
    cle_b = rows(tmp_path / "b.parquet")[0]["cle_vente"]
    assert cle_a == cle_b


def test_l_officiel_prime_sur_l_archive_historique(tmp_path: Path) -> None:
    """L'archive communautaire s'arrete a 2023 : ses annees recentes sont
    tronquees et sans les corrections DVF posterieures."""
    hist = _ecrire(tmp_path / "historique" / "48.csv",
                   [{**VENTE, "id_mutation": "2020-1", "surface_reelle_bati": "90"}])
    off = _ecrire(tmp_path / "dvf" / "2020" / "48.csv",
                  [{**VENTE, "id_mutation": "2020-999", "surface_reelle_bati": "95"}])

    rapport = dvf.prepare([hist, off], tmp_path / "out.parquet")

    assert rapport["mutations"] == 1          # et non 2 : la vente n'est pas dupliquee
    vente = rows(tmp_path / "out.parquet")[0]
    assert vente["surface_bati"] == 95        # la version officielle, pas l'archive
    assert vente["id_mutation"] == "2020-999"


def test_une_vente_absente_de_l_officiel_survit(tmp_path: Path) -> None:
    """Tout l'interet de l'archive : les annees hors fenetre glissante DVF."""
    ancienne = {**VENTE, "date_mutation": "2018-06-01", "id_mutation": "2018-5"}
    hist = _ecrire(tmp_path / "historique" / "48.csv", [ancienne])
    off = _ecrire(tmp_path / "dvf" / "2021" / "48.csv",
                  [{**VENTE, "date_mutation": "2021-06-01", "id_mutation": "2021-7"}])

    rapport = dvf.prepare([hist, off], tmp_path / "out.parquet")

    assert rapport["mutations"] == 2
    assert sorted(r["annee"] for r in rows(tmp_path / "out.parquet")) == [2018, 2021]


def test_la_cle_de_vente_est_unique_meme_en_cas_de_jumelles(tmp_path: Path) -> None:
    """Deux logements d'une meme parcelle vendus le meme jour au meme prix sont
    deux ventes distinctes qui partagent la meme empreinte de contenu.

    Cas reel : 17 sur 17 314 en Lozere. Une cle primaire qui echoue une fois sur
    mille n'en est pas une.
    """
    jumelles = [
        {**VENTE, "id_mutation": "2018-675259", "surface_reelle_bati": "80",
         "code_type_local": "2", "type_local": "Appartement"},
        {**VENTE, "id_mutation": "2018-675261", "surface_reelle_bati": "102",
         "code_type_local": "2", "type_local": "Appartement"},
    ]
    dvf.prepare([_ecrire(tmp_path / "d" / "48.csv", jumelles)], tmp_path / "out.parquet")

    produites = rows(tmp_path / "out.parquet")
    assert len(produites) == 2
    assert len({v["cle_vente"] for v in produites}) == 2

    # Le rang suit la surface, donc il ne depend pas de id_mutation.
    par_surface = {v["surface_bati"]: v["cle_vente"] for v in produites}
    assert par_surface[80.0].endswith("-01")
    assert par_surface[102.0].endswith("-02")
