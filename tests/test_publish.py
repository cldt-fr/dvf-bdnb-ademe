"""Comportements de la mise en Release.

Le DDL est dérivé du schéma Parquet plutôt qu'écrit à la main : ces tests
vérifient que la dérivation reste juste, car un type qui dérive du fichier livré
se paye au chargement, chez le consommateur.
"""

from pathlib import Path

import duckdb
import pytest

from dvf_bdnb import publish


@pytest.fixture
def parquet(tmp_path: Path) -> Path:
    destination = tmp_path / "dvf" / "dept-33.parquet"
    destination.parent.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT '2024-1' AS id_mutation,
                   DATE '2024-03-01' AS date_mutation,
                   CAST(2024 AS SMALLINT) AS annee,
                   '33063' AS code_commune,
                   CAST(250000.0 AS DOUBLE) AS valeur_fonciere,
                   CAST(1234.56 AS DECIMAL(10,2)) AS montant_exact,
                   CAST(3 AS INTEGER) AS nb_pieces,
                   true AS marque,
                   'ok' AS qualite_prix_m2
        ) TO '{destination}' (FORMAT parquet)
    """)
    return destination


def test_les_types_parquet_deviennent_des_types_postgres(parquet: Path) -> None:
    ddl = publish.postgres_ddl(parquet, "dvf")
    assert "date_mutation                      date" in ddl
    assert "annee                              smallint" in ddl
    assert "valeur_fonciere                    double precision" in ddl
    # DECIMAL perd sa precision au passage : `numeric` sans argument est a
    # precision arbitraire sous PostgreSQL, donc rien n'est perdu a l'arrivee.
    assert "montant_exact                      numeric" in ddl
    assert "nb_pieces                          integer" in ddl
    assert "marque                             boolean" in ddl
    assert "id_mutation                        text" in ddl


def test_seules_les_colonnes_presentes_recoivent_un_index(parquet: Path) -> None:
    """Un index sur une colonne absente ferait echouer tout le script."""
    ddl = publish.postgres_ddl(parquet, "dvf")
    assert "dvf_code_commune_idx" in ddl
    assert "dvf_annee_idx" in ddl
    # `parcelle_id` figure dans la liste des colonnes indexables mais pas dans
    # ce jeu : aucun index ne doit etre emis pour elle.
    assert "parcelle_id" not in ddl


def test_le_ddl_rappelle_de_creer_les_index_apres_le_chargement(parquet: Path) -> None:
    """Conseil qui divise le temps de chargement par deux sur un gros departement."""
    assert "APRÈS le chargement" in publish.postgres_ddl(parquet, "dvf")


def test_le_csv_conserve_toutes_les_lignes(parquet: Path, tmp_path: Path) -> None:
    csv_gz = publish.to_csv_gz(parquet, tmp_path / "dept-33.csv.gz")
    con = duckdb.connect()
    depuis_parquet = con.execute(f"SELECT count(*) FROM read_parquet('{parquet}')").fetchone()
    depuis_csv = con.execute(f"SELECT count(*) FROM read_csv('{csv_gz}', header=true)").fetchone()
    assert depuis_parquet == depuis_csv


def test_le_manifeste_porte_de_quoi_verifier_un_telechargement(parquet: Path, tmp_path: Path) -> None:
    """La source amont n'est pas fiable : sans empreinte, un consommateur ne peut
    pas distinguer un fichier intact d'un fichier corrompu en transit."""
    bundles = publish.build(parquet.parent.parent, "2026-02-a")
    manifeste = publish.manifest(bundles, "2026-02-a")

    fichiers = manifeste["sources"]["dvf"]["fichiers"]
    assert {f["fichier"] for f in fichiers} == {"dept-33.parquet", "dept-33.csv.gz"}
    for fichier in fichiers:
        assert len(fichier["sha256"]) == 64
        assert fichier["octets"] > 0
    assert manifeste["sources"]["dvf"]["lignes_totales"] == 1
    assert manifeste["licence"].startswith("Licence Ouverte")


def test_build_produit_le_ddl_a_cote_des_donnees(parquet: Path) -> None:
    out = parquet.parent.parent
    bundles = publish.build(out, "2026-02-a")
    assert bundles[0].ddl == out / "schema" / "dvf.sql"
    assert bundles[0].ddl.exists()
