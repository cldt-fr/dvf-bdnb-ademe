"""Résolution de la liste des territoires, et refus des territoires inconnus."""

import pytest

from dvf_bdnb import departments
from dvf_bdnb.prepare import dpe


def test_une_liste_explicite_est_respectee() -> None:
    assert departments.resolve("33,40,2a", "dvf") == ["33", "40", "2A"]


def test_all_declenche_la_decouverte(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(departments, "discover_dvf", lambda **_: ["01", "02"])
    assert departments.resolve("all", "dvf") == ["01", "02"]
    assert departments.resolve(None, "dvf") == ["01", "02"]


def test_la_liste_de_repli_couvre_les_101_departements() -> None:
    assert len(departments.CANONICAL) == 101
    assert "20" not in departments.CANONICAL     # la Corse est 2A/2B
    assert {"2A", "2B", "976"} <= set(departments.CANONICAL)


def test_un_territoire_inconnu_leve_plutot_que_de_supposer() -> None:
    """C'est exactement l'hypothèse qui rend faux le champ de position de l'ADEME.

    Traiter la Nouvelle-Calédonie comme la Bourgogne place ses diagnostics à des
    milliers de kilomètres, sans la moindre erreur. Mieux vaut refuser.
    """
    with pytest.raises(ValueError, match="inconnu"):
        dpe.srid_for("999")


@pytest.mark.parametrize(
    ("territoire", "srid"),
    [
        ("33", 2154),   # Gironde — Lambert 93
        ("2B", 2154),   # Haute-Corse — Lambert 93
        ("971", 5490),  # Guadeloupe
        ("973", 2972),  # Guyane
        ("974", 2975),  # La Réunion
        ("975", 4467),  # Saint-Pierre-et-Miquelon
        ("976", 4471),  # Mayotte
        ("978", 5490),  # Saint-Martin
        ("988", 3163),  # Nouvelle-Calédonie
    ],
)
def test_chaque_territoire_connu_a_son_systeme(territoire: str, srid: int) -> None:
    """Chacun a été vérifié en reprojetant une coordonnée réelle de l'ADEME."""
    assert dpe.srid_for(territoire) == srid
