"""Mise en Release : CSV, DDL, manifeste.

« Prêt à importer » a deux sens, et on livre les deux :

- **Parquet** pour qui analyse — DuckDB, Python, R, QGIS lisent le fichier tel
  quel, même depuis une URL, sans rien charger.
- **CSV compressé + DDL** pour qui met en base — `COPY` reste la voie la plus
  rapide en volume sous PostgreSQL, à condition d'avoir la table déjà créée avec
  les bons types.

Le DDL est **dérivé du schéma Parquet**, jamais écrit à la main : une colonne
ajoutée en amont se retrouve dans le `CREATE TABLE` sans que personne ait à y
penser, et les types ne peuvent pas diverger du fichier livré.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

# DuckDB -> PostgreSQL. Le typage large est volontaire : mieux vaut un bigint
# trop grand qu'un integer qui déborde au chargement d'un département dense.
TYPES = {
    "BOOLEAN": "boolean",
    "TINYINT": "smallint",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "HUGEINT": "numeric",
    "FLOAT": "real",
    "DOUBLE": "double precision",
    "DECIMAL": "numeric",
    "VARCHAR": "text",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "BLOB": "bytea",
}

# Colonnes qui méritent un index si elles existent. Déclaratif plutôt que codé
# par source : une colonne renommée en amont ne laisse pas un index fantôme.
INDEXED = (
    "code_commune",
    "code_departement",
    "date_mutation",
    "date_dpe",
    "annee",
    "classe_dpe",
    "id_parcelle",
    "parcelle_id",
    "batiment_groupe_id",
    "qualite_prix_m2",
)


@dataclass
class Asset:
    """Un fichier publié, et de quoi vérifier qu'il est arrivé intact."""

    path: Path
    sha256: str
    bytes: int
    rows: int | None = None

    def to_dict(self) -> dict:
        return {
            "fichier": self.path.name,
            "octets": self.bytes,
            "sha256": self.sha256,
            **({"lignes": self.rows} if self.rows is not None else {}),
        }


@dataclass
class Bundle:
    """Ce qu'on publie pour une source et un millésime."""

    source: str
    millesime: str
    assets: list[Asset] = field(default_factory=list)
    ddl: Path | None = None


def to_csv_gz(parquet: Path, destination: Path, connection: duckdb.DuckDBPyConnection | None = None) -> Path:
    """Décline un Parquet en CSV compressé, pour le chargement en base."""
    con = connection or duckdb.connect()
    destination.parent.mkdir(parents=True, exist_ok=True)
    source = str(parquet).replace("'", "''")
    target = str(destination).replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{source}')) TO '{target}' "
        "(FORMAT csv, HEADER, COMPRESSION gzip)"
    )
    return destination


def postgres_ddl(parquet: Path, table: str, connection: duckdb.DuckDBPyConnection | None = None) -> str:
    """Produit le CREATE TABLE, ses index et la commande de chargement."""
    con = connection or duckdb.connect()
    source = str(parquet).replace("'", "''")
    columns = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()

    lines = [f"CREATE TABLE IF NOT EXISTS {table} ("]
    definitions = []
    names = []
    for name, duck_type, *_ in columns:
        names.append(name)
        base = duck_type.split("(")[0].strip().upper()
        definitions.append(f"    {name:34} {TYPES.get(base, 'text')}")
    # Cle primaire, quand le jeu en porte une STABLE.
    #
    # `id_mutation` n'en est pas une : c'est une sequence reattribuee a chaque
    # publication, donc un import incremental cale dessus reduplique tout le jeu
    # au millesime suivant. `cle_vente`, calculee depuis le contenu, tient — et
    # la declarer ici est ce qui rend l'import incremental sur
    # `ON CONFLICT DO UPDATE` possible.
    if "cle_vente" in names:
        definitions.append(f"    {'PRIMARY KEY (cle_vente)':34}")
    elif "numero_dpe" in names:
        definitions.append(f"    {'PRIMARY KEY (numero_dpe)':34}")

    lines.append(",\n".join(definitions))
    lines.append(");")
    lines.append("")

    for column in INDEXED:
        if column in names:
            lines.append(f"CREATE INDEX IF NOT EXISTS {table}_{column}_idx ON {table} ({column});")

    lines += [
        "",
        "-- Chargement. COPY est la voie la plus rapide en volume : il court-circuite",
        "-- le parcours des INSERT individuels. Créer les index APRÈS le chargement",
        "-- divise encore le temps par deux sur un gros département.",
        f"-- zcat {table}-dept-XX.csv.gz | psql -c \"COPY {table} FROM STDIN (FORMAT csv, HEADER)\"",
        "",
        "-- Mise a jour incrementale, a la publication suivante : la cle primaire",
        "-- etant stable d'un millesime a l'autre, une vente corrigee se met a jour",
        "-- au lieu d'etre inseree en double.",
        f"-- COPY {table}_import FROM STDIN (FORMAT csv, HEADER);",
        f"-- INSERT INTO {table} SELECT * FROM {table}_import",
        "--   ON CONFLICT (cle_vente) DO UPDATE SET valeur_fonciere = EXCLUDED.valeur_fonciere;",
        "",
    ]
    return "\n".join(lines)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def describe(path: Path, connection: duckdb.DuckDBPyConnection | None = None) -> Asset:
    rows = None
    if path.suffix == ".parquet":
        con = connection or duckdb.connect()
        source = str(path).replace("'", "''")
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{source}')").fetchone()[0]
    return Asset(path=path, sha256=sha256(path), bytes=path.stat().st_size, rows=rows)


def build(out: Path, millesime: str, sources: list[str] | None = None) -> list[Bundle]:
    """Prépare tout ce qui part en Release, sans rien envoyer."""
    con = duckdb.connect()
    bundles: list[Bundle] = []

    for directory in sorted(p for p in out.iterdir() if p.is_dir()):
        source = directory.name
        if sources and source not in sources:
            continue

        parquets = sorted(directory.glob("*.parquet"))
        if not parquets:
            continue

        bundle = Bundle(source=source, millesime=millesime)
        for parquet in parquets:
            bundle.assets.append(describe(parquet, con))
            csv_gz = to_csv_gz(parquet, parquet.with_suffix(".csv.gz"), con)
            bundle.assets.append(describe(csv_gz, con))

        schema_dir = out / "schema"
        schema_dir.mkdir(parents=True, exist_ok=True)
        ddl_path = schema_dir / f"{source}.sql"
        ddl_path.write_text(postgres_ddl(parquets[0], source, con), encoding="utf-8")
        bundle.ddl = ddl_path
        bundles.append(bundle)

    return bundles


def manifest(bundles: list[Bundle], millesime: str) -> dict:
    """Le manifeste sert deux usages : vérifier un téléchargement, et servir de
    mémoire au pipeline — il dit ce qui a réellement été publié."""
    return {
        "millesime": millesime,
        "publie_le": datetime.now(UTC).isoformat(timespec="seconds"),
        "licence": "Licence Ouverte 2.0 (Etalab)",
        "sources": {
            b.source: {
                "fichiers": [a.to_dict() for a in b.assets],
                "ddl": b.ddl.name if b.ddl else None,
                "lignes_totales": sum(a.rows or 0 for a in b.assets if a.path.suffix == ".parquet"),
            }
            for b in bundles
        },
    }


def write_manifest(bundles: list[Bundle], millesime: str, destination: Path) -> Path:
    destination.write_text(
        json.dumps(manifest(bundles, millesime), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination
