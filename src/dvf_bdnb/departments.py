"""Liste des départements et territoires à traiter.

Découverte plutôt que codée en dur : l'index DVF et l'API de l'ADEME savent
eux-mêmes ce qu'ils publient. C'est aussi ainsi qu'on apprend qu'un territoire
est apparu — plutôt que de le découvrir en produisant des coordonnées fausses.
"""

from __future__ import annotations

import re

import httpx

# Repli quand une source ne s'expose pas : les 101 départements français.
# 20 n'existe pas (la Corse est 2A/2B), et 976 est Mayotte.
CANONICAL: tuple[str, ...] = tuple(
    [f"{n:02d}" for n in range(1, 96) if n != 20]
    + ["2A", "2B", "971", "972", "973", "974", "976"]
)

DVF_INDEX = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/"
DPE_VALUES = "{api}/values/code_departement_ban?size=200"


def discover_dvf(year: str = "2025", timeout: float = 30.0) -> list[str]:
    """Départements réellement publiés par geo-dvf pour une année.

    L'index omet le Bas-Rhin, le Haut-Rhin, la Moselle et Mayotte : ces
    territoires relèvent du livre foncier ou n'ont pas de publication. Se fier à
    l'index évite d'aller chercher des fichiers qui n'existent pas.
    """
    try:
        response = httpx.get(DVF_INDEX.format(year=year), follow_redirects=True, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError:
        return list(CANONICAL)

    found = re.findall(r'href="[^"]*/([0-9AB]{2,3})\.csv\.gz"', response.text)
    return sorted(set(found)) or list(CANONICAL)


def discover_dpe(api_url: str, timeout: float = 30.0) -> list[str]:
    """Territoires exposés par l'ADEME.

    Elle en publie davantage que la métropole et les DOM : Saint-Pierre-et-
    Miquelon, Saint-Martin, la Nouvelle-Calédonie. Chacun a son propre système
    de coordonnées, et les traiter par défaut placerait leurs diagnostics à des
    milliers de kilomètres.
    """
    try:
        response = httpx.get(DPE_VALUES.format(api=api_url), follow_redirects=True, timeout=timeout)
        response.raise_for_status()
        values = response.json()
    except (httpx.HTTPError, ValueError):
        return list(CANONICAL)

    if not isinstance(values, list):
        return list(CANONICAL)
    return sorted({str(v) for v in values if v})


def resolve(requested: str | None, source: str, api_url: str | None = None) -> list[str]:
    """Traduit l'option de ligne de commande en liste de territoires."""
    if requested and requested.lower() not in ("all", "tous"):
        return [d.strip().upper() for d in requested.split(",") if d.strip()]

    if source == "dvf":
        return discover_dvf()
    if source == "dpe" and api_url:
        return discover_dpe(api_url)
    return list(CANONICAL)
