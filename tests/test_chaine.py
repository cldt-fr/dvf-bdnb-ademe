"""Garde-fous de la chaîne complète.

Ces deux tests couvrent des bugs réels, trouvés en lançant la chaîne sur le jeu
entier — aucun test existant ne passait par ces chemins.
"""

import inspect
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from dvf_bdnb import fetch
from dvf_bdnb.cli import app

runner = CliRunner()


def test_la_pagination_existe_et_est_un_generateur() -> None:
    """La chaîne appelait `fetch.paginate`, qui n'existait plus.

    Les 104 territoires ont échoué sur « module has no attribute ». Aucun test
    ne passait par là : ils appelaient tous la préparation directement, avec des
    lignes déjà en mémoire.
    """
    assert hasattr(fetch, "paginate")
    assert inspect.isgeneratorfunction(fetch.paginate)


def test_une_source_sans_aucune_donnee_bloque(tmp_path: Path) -> None:
    """Le trou le plus grave rencontré.

    Les contrôles ne regardaient que les fichiers existants. Une source qui
    échoue INTÉGRALEMENT ne produit rien — donc rien à contrôler — et la chaîne
    annonçait « tous les contrôles passent » sur un échec total, prête à
    publier.
    """
    (tmp_path / "dvf").mkdir()
    duckdb.connect().execute(f"""
        COPY (SELECT 48.85 AS latitude, 2.35 AS longitude)
        TO '{tmp_path / "dvf" / "dept-75.parquet"}' (FORMAT parquet)
    """)

    # Sans exigence : le jeu DVF seul suffit à faire passer les contrôles.
    assert runner.invoke(app, ["verify", "--out", str(tmp_path)]).exit_code == 0

    # En exigeant le DPE : son absence doit bloquer.
    resultat = runner.invoke(app, ["verify", "--out", str(tmp_path), "--expect", "dvf,dpe"])
    assert resultat.exit_code == 1
    assert "aucune donnee produite" in resultat.stdout


def test_les_controles_passent_quand_tout_est_la(tmp_path: Path) -> None:
    con = duckdb.connect()
    for source in ("dvf", "dpe"):
        (tmp_path / source).mkdir()
        con.execute(f"""
            COPY (SELECT 48.85 AS latitude, 2.35 AS longitude)
            TO '{tmp_path / source / "dept-75.parquet"}' (FORMAT parquet)
        """)

    resultat = runner.invoke(app, ["verify", "--out", str(tmp_path), "--expect", "dvf,dpe"])
    assert resultat.exit_code == 0
