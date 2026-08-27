"""Lecture du registre declaratif des sources."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Source:
    name: str
    label: str
    producer: str
    licence: str
    cadence: str
    format: str
    urls: dict[str, str]
    verify: str | None = None

    def url(self, key: str, **params: object) -> str:
        """URL du registre, avec ses parametres substitues."""
        template = self.urls.get(key)
        if template is None:
            raise KeyError(f"la source « {self.name} » n'a pas d'URL « {key} »")
        return template.format(**params)


def registry(path: Path | None = None) -> dict[str, Source]:
    """Charge sources.toml.

    Le registre est declaratif pour que l'ajout d'une annee ou le changement
    d'une URL ne demande jamais de toucher au code.
    """
    path = path or _default_registry_path()
    raw = tomllib.loads(path.read_text(encoding="utf-8"))

    sources: dict[str, Source] = {}
    for name, entry in raw.items():
        urls = {k.removesuffix("_url"): v for k, v in entry.items() if k.endswith("_url")}
        sources[name] = Source(
            name=name,
            label=entry["label"],
            producer=entry["producer"],
            licence=entry["licence"],
            cadence=entry.get("cadence", "inconnue"),
            format=entry.get("format", "inconnu"),
            urls=urls,
            verify=entry.get("verify"),
        )
    return sources


def _default_registry_path() -> Path:
    """Registre embarqué dans le paquet.

    Il vit à côté du code, et non à la racine du dépôt : sinon il n'est pas
    livré avec le paquet installé, et l'outil échoue au premier lancement chez
    quelqu'un d'autre — en n'ayant jamais échoué chez soi.

    `DVF_BDNB_SOURCES` permet de pointer un autre registre, pour essayer une URL
    sans réinstaller.
    """
    override = os.environ.get("DVF_BDNB_SOURCES")
    if override:
        path = Path(override).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"registre introuvable : {path}")
        return path

    embedded = Path(__file__).with_name("sources.toml")
    if embedded.exists():
        return embedded

    raise FileNotFoundError(
        "sources.toml introuvable dans le paquet. Réinstaller, ou pointer un "
        "registre avec la variable DVF_BDNB_SOURCES."
    )
