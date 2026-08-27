"""Extraction selective d'une archive BDNB.

Ce chemin a casse en production sur un `AttributeError` : l'appelant existait,
la fonction non, et aucun test ne parcourait le chemin BDNB de bout en bout.
"""

from __future__ import annotations

import io
import tarfile

import httpx
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


# --- Reprise a l'octet -------------------------------------------------------
#
# L'archive CSV de la BDNB fait 40 Go et une seule requete GET ne tient pas :
# trois tentatives d'affilee sont mortes entre 5,7 et 7,5 Go. Repartir de zero
# ne converge jamais ; il faut reprendre la ou la lecture s'est arretee.


class _FausseReponse:
    def __init__(self, corps: bytes, debut: int, coupe_apres: int | None):
        self.status_code = 206 if debut else 200
        self._corps = corps
        self._coupe = coupe_apres

    def raise_for_status(self):
        return None

    def iter_bytes(self, taille):
        envoye = 0
        for i in range(0, len(self._corps), taille):
            bloc = self._corps[i:i + taille]
            if self._coupe is not None and envoye + len(bloc) > self._coupe:
                bloc = bloc[: self._coupe - envoye]
                if bloc:
                    yield bloc
                raise httpx.ReadError("connexion interrompue")
            envoye += len(bloc)
            yield bloc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FauxClient:
    """Sert un corps en memoire, en coupant la premiere requete a mi-parcours."""

    def __init__(self, corps: bytes, journal: list):
        self.corps = corps
        self.journal = journal

    def stream(self, methode, url, headers=None):
        debut = 0
        if headers and "Range" in headers:
            debut = int(headers["Range"].split("=")[1].split("-")[0])
        self.journal.append(debut)
        premiere = len(self.journal) == 1
        return _FausseReponse(
            self.corps[debut:], debut,
            coupe_apres=len(self.corps) // 3 if premiere else None,
        )

    def close(self):
        return None


def test_la_lecture_reprend_a_l_octet_apres_une_coupure(monkeypatch) -> None:
    import httpx as _httpx

    corps = bytes(range(256)) * 400          # 102 400 octets, contenu verifiable
    journal: list[int] = []
    monkeypatch.setattr(_httpx, "Client", lambda **k: _FauxClient(corps, journal))

    lecteur = fetch._ResumableReader("https://exemple.invalid/a.tar.gz", len(corps))
    lu = lecteur.read(-1)
    lecteur.close()

    assert lu == corps, "le flux recolle doit etre identique a l'original"
    assert len(journal) == 2, "il faut exactement une reprise"
    assert journal[0] == 0
    assert journal[1] == len(corps) // 3, "la reprise doit repartir de l'octet atteint"


def test_la_reprise_est_signalee_et_non_silencieuse(monkeypatch) -> None:
    """Sans trace, le compteur d'octets semble ralentir sans raison."""
    import httpx as _httpx

    corps = b"z" * 30_000
    monkeypatch.setattr(_httpx, "Client", lambda **k: _FauxClient(corps, []))

    vues: list[tuple[int, str]] = []
    lecteur = fetch._ResumableReader(
        "https://exemple.invalid/a.tar.gz", len(corps),
        on_resume=lambda octets, raison: vues.append((octets, raison)),
    )
    lecteur.read(-1)
    lecteur.close()

    assert len(vues) == 1
    assert vues[0][0] == 10_000


def test_une_source_sans_reprise_par_plage_est_denoncee(monkeypatch) -> None:
    """Reprendre sans que le serveur honore Range renverrait le fichier ENTIER
    recolle apres un prefixe deja lu — donc une archive silencieusement fausse."""
    import httpx as _httpx

    corps = b"y" * 30_000

    class _ClientSansRange(_FauxClient):
        def stream(self, methode, url, headers=None):
            premiere = not self.journal
            self.journal.append(0)
            reponse = _FausseReponse(self.corps, 0, len(corps) // 3 if premiere else None)
            reponse.status_code = 200      # ignore le Range demande
            return reponse

    monkeypatch.setattr(_httpx, "Client", lambda **k: _ClientSansRange(corps, []))

    lecteur = fetch._ResumableReader("https://exemple.invalid/a.tar.gz", len(corps))
    with pytest.raises(OSError, match="reprise par plage"):
        lecteur.read(-1)
