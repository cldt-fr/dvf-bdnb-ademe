"""Extraction selective d'une archive BDNB.

Ce chemin a casse en production sur un `AttributeError` : l'appelant existait,
la fonction non, et aucun test ne parcourait le chemin BDNB de bout en bout.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from dvf_bdnb import fetch


def archive(contenu: dict[str, bytes], prefixe: str = "./csv/") -> io.BytesIO:
    tampon = io.BytesIO()
    with tarfile.open(fileobj=tampon, mode="w:gz") as tar:
        for nom, octets in contenu.items():
            info = tarfile.TarInfo(f"{prefixe}{nom}")
            info.size = len(octets)
            tar.addfile(info, io.BytesIO(octets))
    tampon.seek(0)
    return tampon


CSV = b"id,valeur\n1,a\n"


def test_extrait_seulement_les_tables_voulues(tmp_path: Path) -> None:
    flux = archive({
        "batiment_groupe.csv": CSV,
        "table_inutile.csv": CSV,
        "rel_batiment_groupe_parcelle.csv": CSV,
    })

    trouves = fetch.extract_from_stream(
        flux, tmp_path,
        ["./csv/batiment_groupe.csv", "./csv/rel_batiment_groupe_parcelle.csv"],
    )

    assert sorted(Path(t).name for t in trouves) == [
        "batiment_groupe.csv", "rel_batiment_groupe_parcelle.csv",
    ]
    assert not (tmp_path / "table_inutile.csv").exists()


def test_le_prefixe_de_l_archive_est_indifferent(tmp_path: Path) -> None:
    """Le millesime change le prefixe des entrees ; le nom de fichier, non."""
    flux = archive({"batiment_groupe.csv": CSV}, prefixe="millesime_2026/csv/")

    trouves = fetch.extract_from_stream(flux, tmp_path, ["./csv/batiment_groupe.csv"])

    assert len(trouves) == 1
    assert (tmp_path / "batiment_groupe.csv").read_bytes() == CSV


def test_un_membre_corrompu_est_refuse_et_non_ecrit(tmp_path: Path) -> None:
    """La source BDNB corrompt par intermittence ; mieux vaut rien que du faux."""
    flux = archive({"batiment_groupe.csv": b"\x00\x00\x00binaire"})

    with pytest.raises(OSError, match="illisible"):
        fetch.extract_from_stream(flux, tmp_path, ["./csv/batiment_groupe.csv"])

    assert not (tmp_path / "batiment_groupe.csv").exists()
    assert list(tmp_path.iterdir()) == []   # pas de .part laisse derriere


def test_s_arrete_des_que_tout_est_sorti(tmp_path: Path) -> None:
    """Sur 40 Go, lire la suite pour rien coute des dizaines de minutes."""
    enorme = b"x,y\n" + b"9,9\n" * 200_000
    flux = archive({"batiment_groupe.csv": CSV, "apres.csv": enorme})

    fetch.extract_from_stream(flux, tmp_path, ["./csv/batiment_groupe.csv"])

    assert not (tmp_path / "apres.csv").exists()


def test_reprise_ne_re_extrait_pas_ce_qui_est_deja_la(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "batiment_groupe.csv").write_bytes(CSV)

    def jamais_appele(*a, **k):
        raise AssertionError("le reseau ne devrait pas etre sollicite")

    monkeypatch.setattr(fetch, "probe", jamais_appele)

    trouves = fetch.extract_members(
        "https://exemple.invalid/archive.tar.gz", tmp_path,
        ["./csv/batiment_groupe.csv"],
    )
    assert len(trouves) == 1
