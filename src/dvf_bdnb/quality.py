"""Contrôles qualité, bloquants avant publication.

C'est la contrepartie indispensable de l'automatisation. Une chaîne qui publie
toute seule publie aussi ses propres erreurs — à heure fixe, et avec l'autorité
d'un jeu de référence. Mieux vaut ne rien publier qu'un jeu faux.

Trois familles de contrôles :

- **Structure** : le fichier existe, il a des lignes, ses colonnes sont là.
- **Plausibilité** : les coordonnées tombent dans leur territoire, les dates dans
  leur fenêtre. C'est ce contrôle qui aurait attrapé seul le `_geopoint` de
  l'ADEME, qui place les diagnostics réunionnais en mer du Nord.
- **Régression** : comparaison au millésime précédent. Un jeu qui perd la moitié
  de ses lignes du jour au lendemain signale un problème amont, pas une
  évolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import duckdb


class Level(str, Enum):
    OK = "ok"
    WARN = "avertissement"
    FAIL = "echec"


@dataclass
class Finding:
    level: Level
    check: str
    message: str
    detail: dict | None = None

    def __str__(self) -> str:
        marque = {Level.OK: "  ok  ", Level.WARN: " warn ", Level.FAIL: " ECHEC"}[self.level]
        return f"[{marque}] {self.check} — {self.message}"


@dataclass
class Report:
    findings: list[Finding]

    @property
    def blocking(self) -> list[Finding]:
        return [f for f in self.findings if f.level is Level.FAIL]

    @property
    def passed(self) -> bool:
        return not self.blocking

    def to_dict(self) -> dict:
        return {
            "verdict": "publiable" if self.passed else "bloque",
            "controles": [
                {"niveau": f.level.value, "controle": f.check, "message": f.message,
                 **({"detail": f.detail} if f.detail else {})}
                for f in self.findings
            ],
        }


# Emprises géographiques par territoire. Un point hors de son emprise trahit une
# reprojection ratée — l'erreur la plus coûteuse du domaine, parce qu'elle ne
# lève aucune exception et se voit seulement sur une carte.
BOUNDS = {
    "metropole": (41.0, 51.5, -5.5, 9.8),
    "971": (15.7, 16.6, -61.9, -60.9),   # Guadeloupe
    "972": (14.3, 15.0, -61.3, -60.7),   # Martinique
    "973": (2.0, 6.0, -55.0, -51.5),     # Guyane
    "974": (-21.5, -20.8, 55.1, 55.9),   # La Réunion
    "976": (-13.1, -12.6, 45.0, 45.3),   # Mayotte
}

# DVF ne couvre pas ces départements : livre foncier d'Alsace-Moselle, et Mayotte.
# Un compte nul y est normal et ne doit pas bloquer la publication.
DVF_UNCOVERED = {"57", "67", "68", "976"}

# En deçà, une chute de volume signale un problème amont plutôt qu'une évolution.
MAX_VOLUME_DROP = 0.20


def bounds_for(department: str) -> tuple[float, float, float, float]:
    return BOUNDS.get(department, BOUNDS["metropole"])


def check_dataset(
    parquet: Path,
    source: str,
    department: str,
    *,
    baseline: dict | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Report:
    """Passe un fichier préparé au crible."""
    con = connection or duckdb.connect()
    findings: list[Finding] = []
    label = f"{source}/{department}"

    if not parquet.exists():
        return Report([Finding(Level.FAIL, "presence", f"{label} : fichier absent")])

    path = str(parquet).replace("'", "''")
    columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]

    findings.append(_check_volume(rows, source, department, label))
    if rows == 0:
        return Report(findings)

    if {"latitude", "longitude"} <= columns:
        findings.append(_check_bounds(con, path, department, label))

    if baseline is not None:
        findings.append(_check_regression(rows, baseline, label))

    return Report(findings)


def _check_volume(rows: int, source: str, department: str, label: str) -> Finding:
    if rows > 0:
        return Finding(Level.OK, "volume", f"{label} : {rows} lignes")

    if source == "dvf" and department in DVF_UNCOVERED:
        # Le livre foncier d'Alsace-Moselle et Mayotte sont hors DVF : un compte
        # nul y est attendu, et le confondre avec une panne ferait bloquer une
        # publication parfaitement valide.
        return Finding(
            Level.OK, "volume",
            f"{label} : vide, ce qui est normal (hors couverture DVF)",
        )

    return Finding(Level.FAIL, "volume", f"{label} : aucune ligne")


def _check_bounds(con: duckdb.DuckDBPyConnection, path: str, department: str, label: str) -> Finding:
    lat_min, lat_max, lon_min, lon_max = bounds_for(department)

    row = con.execute(f"""
        SELECT count(*) FILTER (latitude IS NOT NULL),
               count(*) FILTER (
                   latitude IS NOT NULL AND (
                       latitude NOT BETWEEN {lat_min} AND {lat_max}
                       OR longitude NOT BETWEEN {lon_min} AND {lon_max})),
               min(latitude), max(latitude), min(longitude), max(longitude)
        FROM read_parquet('{path}')
    """).fetchone()

    localises, hors_emprise, la_min, la_max, lo_min, lo_max = row

    if localises == 0:
        return Finding(Level.WARN, "emprise", f"{label} : aucun point localisé")

    detail = {
        "emprise_attendue": {"lat": [lat_min, lat_max], "lon": [lon_min, lon_max]},
        "emprise_observee": {"lat": [la_min, la_max], "lon": [lo_min, lo_max]},
        "hors_emprise": hors_emprise,
    }
    part = hors_emprise / localises

    if part == 0:
        return Finding(Level.OK, "emprise", f"{label} : tous les points dans le territoire")
    if part < 0.001:
        return Finding(
            Level.WARN, "emprise",
            f"{label} : {hors_emprise} point(s) hors emprise, sans doute des saisies isolées",
            detail,
        )
    return Finding(
        Level.FAIL, "emprise",
        f"{label} : {part:.1%} des points hors du territoire — reprojection probablement fausse",
        detail,
    )


def _check_regression(rows: int, baseline: dict, label: str) -> Finding:
    previous = baseline.get("lignes")
    if not previous:
        return Finding(Level.OK, "regression", f"{label} : pas de millésime précédent, rien à comparer")

    variation = (rows - previous) / previous
    detail = {"precedent": previous, "actuel": rows, "variation": round(variation * 100, 1)}

    if variation < -MAX_VOLUME_DROP:
        return Finding(
            Level.FAIL, "regression",
            f"{label} : {variation:.1%} de lignes par rapport au millésime précédent",
            detail,
        )
    if variation < 0:
        return Finding(
            Level.WARN, "regression",
            f"{label} : léger recul de {variation:.1%}",
            detail,
        )
    return Finding(Level.OK, "regression", f"{label} : {variation:+.1%}", detail)


def baseline_from_manifest(manifest: dict) -> dict[str, dict]:
    """Extrait de quoi comparer, depuis le manifeste du millésime précédent.

    Le manifeste publié est la seule mémoire fiable de ce qui existe vraiment :
    un fichier d'état local peut diverger de la réalité, lui non.
    """
    baseline: dict[str, dict] = {}
    for source, contenu in manifest.get("sources", {}).items():
        for fichier in contenu.get("fichiers", []):
            nom = fichier["fichier"]
            if not nom.endswith(".parquet"):
                continue
            department = nom.removeprefix("dept-").removesuffix(".parquet")
            baseline[f"{source}/{department}"] = {"lignes": fichier.get("lignes")}
    return baseline
