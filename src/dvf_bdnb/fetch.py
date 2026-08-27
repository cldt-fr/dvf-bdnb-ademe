"""Telechargement qui ne fait confiance a aucune source.

MOTIF, constate en conditions reelles sur la BDNB : le stockage objet renvoie
par moments des octets faux sur les grosses lectures, SANS erreur HTTP — code
200, longueur correcte, contenu different. Trois lectures successives de la meme
plage de 64 Mo ont donne trois empreintes differentes, et deux telechargements
complets d'affilee ont produit des archives inutilisables.

Pour un pipeline automatique, c'est le risque numero un : publier des donnees
fausses tout seul est bien pire que ne rien publier. D'ou la lecture par tronçons
avec double lecture concordante — un desaccord relance le tronçon, jamais les
40 Go.
"""

from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx

CHUNK = 256 * 1024 * 1024
MAX_CHUNK_ATTEMPTS = 8
TIMEOUT = httpx.Timeout(60.0, read=300.0)


@dataclass
class RemoteInfo:
    """Ce que la source dit d'elle-meme, avant tout telechargement."""

    size: int | None
    etag: str | None
    last_modified: str | None
    # Un fichier se mesure en octets, un jeu expose par API en lignes. Confondre
    # les deux affiche « 0.0 Go » pour 15 millions de diagnostics.
    unit: str = "bytes"
    # Cadence annoncee par la source elle-meme, quand elle la publie.
    frequency: str | None = None

    def human_size(self) -> str:
        if self.size is None:
            return self.last_modified or "sans marque"
        if self.unit == "rows":
            return f"{self.size:,} lignes".replace(",", " ")
        if self.size >= 1_000_000_000:
            return f"{self.size / 1e9:.1f} Go"
        return f"{self.size / 1e6:.1f} Mo"


def probe(url: str) -> RemoteInfo:
    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client:
        response = client.head(url)
        response.raise_for_status()
        size = response.headers.get("content-length")
        return RemoteInfo(
            size=int(size) if size else None,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
        )


def probe_dataset(url: str) -> RemoteInfo:
    """Marque de version d'un jeu expose par API plutot que par fichier.

    L'ADEME ne publie pas d'archive a telecharger : elle expose un jeu qui porte
    une date de derniere modification et un nombre de lignes. Les deux ensemble
    font une marque de version aussi fiable qu'un ETag.
    """
    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()

    # `dataUpdatedAt` d'abord, et surtout PAS `updatedAt` : le second date la
    # derniere modification des METADONNEES, pas des donnees. Sur le jeu DPE, il
    # etait fige depuis deux mois pendant que les donnees changeaient chaque
    # semaine — de quoi annoncer « inchange » tout ce temps.
    updated = payload.get("dataUpdatedAt") or payload.get("updatedAt")
    count = payload.get("count")
    return RemoteInfo(
        size=int(count) if count is not None else None,
        # Le couple date + nombre de lignes fait une marque de version aussi
        # fiable qu'un ETag : une republication a l'identique ne declenche rien.
        etag=f"{updated}:{count}" if updated else None,
        last_modified=updated,
        unit="rows",
        frequency=payload.get("frequency"),
    )


def download(
    url: str,
    destination: Path,
    *,
    verify_double_read: bool = False,
    on_progress: Callable[[int, int | None], None] | None = None,
) -> Path:
    """Telecharge `url` vers `destination`.

    `verify_double_read` lit chaque tronçon deux fois et n'accepte que si les
    deux lectures concordent. A n'activer que pour les sources connues pour
    corrompre : cela double la bande passante.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    info = probe(url)

    if not verify_double_read or info.size is None:
        return _download_streaming(url, destination, info, on_progress)

    return _download_verified(url, destination, info, on_progress)


def _download_streaming(
    url: str, destination: Path, info: RemoteInfo, on_progress: Callable | None
) -> Path:
    written = 0
    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            with destination.open("wb") as handle:
                for block in response.iter_bytes(1024 * 1024):
                    handle.write(block)
                    written += len(block)
                    if on_progress:
                        on_progress(written, info.size)

    if info.size is not None and written != info.size:
        destination.unlink(missing_ok=True)
        raise OSError(
            f"telechargement incomplet : {written} octets sur {info.size} attendus. "
            "Le fichier a ete supprime — une reprise sur un fichier tronque ne le repare pas."
        )
    return destination


def _download_verified(
    url: str, destination: Path, info: RemoteInfo, on_progress: Callable | None
) -> Path:
    total = info.size
    assert total is not None

    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client, \
         destination.open("wb") as handle:
        offset = 0
        while offset < total:
            end = min(offset + CHUNK - 1, total - 1)
            block = _fetch_agreed_chunk(client, url, offset, end)
            handle.write(block)
            offset = end + 1
            if on_progress:
                on_progress(offset, total)

    return destination


def _fetch_agreed_chunk(client: httpx.Client, url: str, start: int, end: int) -> bytes:
    """Un tronçon lu deux fois, accepte seulement si les deux lectures concordent."""
    expected = end - start + 1

    for _ in range(MAX_CHUNK_ATTEMPTS):
        first = _fetch_range(client, url, start, end)
        if first is None or len(first) != expected:
            continue

        second = _fetch_range(client, url, start, end)
        if second is None or len(second) != expected:
            continue

        if first == second:
            return first
        # Desaccord : la source a renvoye deux contenus differents pour la meme
        # plage. On relance plutot que de choisir arbitrairement.

    raise OSError(
        f"la source n'a pas renvoye deux fois le meme contenu pour les octets {start}-{end} "
        f"apres {MAX_CHUNK_ATTEMPTS} tentatives."
    )


def _fetch_range(client: httpx.Client, url: str, start: int, end: int) -> bytes | None:
    try:
        response = client.get(url, headers={"Range": f"bytes={start}-{end}"})
    except httpx.HTTPError:
        return None
    if response.status_code not in (200, 206):
        return None
    return response.content


def paginated_json(
    url: str,
    params: dict[str, object],
    *,
    on_page: Callable[[int], None] | None = None,
) -> list[dict]:
    """Parcourt une API data-fair page par page.

    La pagination profonde passe par l'URL , qui embarque deja tous les
    parametres : on ne les repasse donc pas, sous peine de repartir du debut.
    """
    rows: list[dict] = []
    next_url: str | None = url
    query: dict[str, object] | None = params

    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client:
        while next_url:
            response = client.get(next_url, params=query)
            response.raise_for_status()
            payload = response.json()
            rows.extend(payload.get("results", []))
            if on_page:
                on_page(len(rows))
            next_url = payload.get("next")
            query = None

    return rows


def extract_members(archive_url: str, destination: Path, members: list[str]) -> list[Path]:
    """Extrait quelques membres d'une archive tar.gz distante, en un seul passage.

    L'archive BDNB pèse ~39 Go et contient une centaine de tables ; on n'en veut
    que cinq. On diffuse donc le flux dans `tar` plutôt que de tout écrire sur
    disque pour ne garder qu'une fraction.

    `--occurrence=1` laisse tar s'arrêter dès qu'il a trouvé chaque membre
    demandé, au lieu de lire l'archive jusqu'au bout.
    """
    destination.mkdir(parents=True, exist_ok=True)

    command = [
        "tar", "-xz", "--occurrence=1", "-C", str(destination),
        "--strip-components=2", *members,
    ]

    with httpx.Client(follow_redirects=True, timeout=TIMEOUT, http2=False) as client:
        with client.stream("GET", archive_url) as response:
            response.raise_for_status()
            process = subprocess.Popen(command, stdin=subprocess.PIPE)
            try:
                for block in response.iter_bytes(4 * 1024 * 1024):
                    process.stdin.write(block)
            except BrokenPipeError:
                # tar a trouve tout ce qu'on lui demandait et s'est arrete : ce
                # n'est pas une erreur, c'est l'effet recherche.
                pass
            finally:
                if process.stdin:
                    process.stdin.close()
                process.wait()

    extracted = [destination / Path(m).name for m in members]
    return [p for p in extracted if p.exists()]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
