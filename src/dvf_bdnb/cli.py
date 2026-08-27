"""Interface en ligne de commande."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from concurrent.futures import ThreadPoolExecutor, as_completed

from dvf_bdnb import departments as depts_module, fetch, publish as publisher, quality, sources as registry
from dvf_bdnb.prepare import (
    bdnb as prepare_bdnb,
    dpe as prepare_dpe,
    dvf as prepare_dvf,
    joint as prepare_joint,
)
from dvf_bdnb.state import State, SourceState

app = typer.Typer(
    add_completion=False,
    help="DVF, BDNB et DPE prepares et republies a chaque millesime.",
)

WORK = Path("work")
STATE_FILE = Path("state.json")


@app.command()
def check(
    source: str = typer.Option(None, help="Ne verifier qu'une source."),
    state_file: Path = typer.Option(STATE_FILE, help="Fichier d'etat des millesimes connus."),
) -> None:
    """Une nouvelle version des sources est-elle parue ?

    Aucun producteur ne publie de notification : on compare l'empreinte de
    chaque source a ce qu'on en connait. Code de sortie 0 si du nouveau est
    disponible, 1 sinon — de quoi enchainer depuis un cron.
    """
    catalogue = registry.registry()
    state = State(state_file)
    names = [source] if source else list(catalogue)
    something_new = False

    for name in names:
        entry = catalogue.get(name)
        if entry is None:
            typer.secho(f"source inconnue : {name}", fg=typer.colors.RED)
            raise typer.Exit(2)

        try:
            info = _probe_source(entry)
        except Exception as error:  # noqa: BLE001 — on veut le motif, pas la trace
            typer.secho(f"{name:6} : source injoignable ({error})", fg=typer.colors.YELLOW)
            continue

        known = state.get(name)
        changed = known.differs_from(info.etag, info.last_modified, info.size)
        something_new = something_new or changed

        marker = "NOUVEAU" if changed else "inchange"
        cadence = f"  (maj {info.frequency})" if info.frequency else ""
        typer.echo(f"{name:6} : {marker:8} — {info.human_size()}{cadence}")

        if changed:
            state.set(name, SourceState(etag=info.etag, last_modified=info.last_modified, size=info.size))

    state.save()
    raise typer.Exit(0 if something_new else 1)


def _prepare_dvf_one(
    entry: registry.Source,
    dept: str,
    years: list[str],
    out: Path,
    force: bool = False,
    historique: registry.Source | None = None,
) -> str:
    destination = out / "dvf" / f"dept-{dept}.parquet"
    if destination.exists() and not force:
        return "deja produit (--force pour refaire)"

    downloaded: list[Path] = []

    # Annees anterieures a la fenetre glissante DVF. Le repertoire s'appelle
    # « historique » : la preparation s'en sert pour faire primer l'officiel.
    if historique is not None:
        archive = WORK / "dvf" / "historique" / f"{dept}.csv"
        if archive.exists():
            downloaded.append(archive)
        else:
            url = fetch.datagouv_department_resources(historique.url("dataset")).get(dept)
            if url:  # absent pour l'outre-mer : ce n'est pas une erreur
                try:
                    fetch.download(url, archive)
                    downloaded.append(archive)
                except Exception:  # noqa: BLE001
                    pass
    for year in years:
        target = WORK / "dvf" / year / f"{dept}.csv.gz"
        if target.exists():
            downloaded.append(target)
            continue
        try:
            fetch.download(entry.url("file", year=year, dept=dept), target)
            downloaded.append(target)
        except Exception:  # noqa: BLE001
            # Une annee absente pour un departement est normale : certaines ne
            # commencent qu'en 2022. On ne fait pas echouer pour autant.
            continue

    if not downloaded:
        raise FileNotFoundError("aucune annee disponible")

    report = prepare_dvf.prepare(downloaded, destination)
    return f"{report['mutations']} mutations, {report['part_geolocalisee']} % geolocalisees"


def _run_parallel(items: list[str], jobs: int, work) -> int:
    """Traite les territoires en parallele, sans qu'un echec emporte les autres.

    Sur un jeu complet, un territoire indisponible ne doit pas condamner les
    cent autres : on collecte les echecs et on les recapitule a la fin.
    """
    reussites, echecs = 0, []

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futures = {pool.submit(work, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                resume = future.result()
                reussites += 1
                typer.secho(f"  {item:4} {resume}", fg=typer.colors.GREEN)
            except Exception as error:  # noqa: BLE001
                echecs.append((item, str(error)[:90]))
                typer.secho(f"  {item:4} echec : {str(error)[:90]}", fg=typer.colors.RED)

    typer.echo("")
    typer.secho(f"{reussites} territoire(s) traites.", fg=typer.colors.GREEN, bold=True)
    if echecs:
        typer.secho(f"{len(echecs)} en echec :", fg=typer.colors.RED, bold=True)
        for item, motif in echecs:
            typer.echo(f"  {item} : {motif}")
    return len(echecs)


def _prepare_dpe(entry: registry.Source, departments: list[str], out: Path, jobs: int = 2, force: bool = False) -> None:
    """Un territoire a la fois : l'API pagine, et charger la France entiere en
    memoire n'aurait pas de sens."""
    echecs = _run_parallel(departments, jobs, lambda d: _prepare_dpe_one(entry, d, out, force))
    if echecs and echecs == len(departments):
        raise typer.Exit(1)


def _prepare_dpe_one(entry: registry.Source, dept: str, out: Path, force: bool = False) -> str:
    destination = out / "dpe" / f"dept-{dept}.parquet"
    if destination.exists() and not force:
        return "deja produit (--force pour refaire)"

    scratch = WORK / "dpe" / dept
    pages = fetch.download_csv_pages(
        entry.url("api") + "/lines",
        {"size": 10000, "select": ",".join(prepare_dpe.COLUMNS),
         "qs": f'code_departement_ban:"{dept}"'},
        scratch,
    )
    report = prepare_dpe.prepare_from_csv(pages, dept, destination)
    return f"{report['diagnostics']} diagnostics, {report['part_bien_positionnee']} % positionnes"


def _prepare_dpe_legacy(entry: registry.Source, departments: list[str], out: Path) -> None:
    for dept in departments:
        typer.echo(f"{dept} : telechargement…")
        rows = fetch.paginated_json(
            entry.url("api") + "/lines",
            {
                "size": 10000,
                "select": ",".join(prepare_dpe.COLUMNS),
                "sort": "numero_dpe",
                "qs": f'code_departement_ban:"{dept}"',
            },
            on_page=lambda n: typer.echo(f"  {n} lignes lues", nl=False, err=True) or typer.echo("\r", nl=False, err=True),
        )
        typer.echo("")

        destination = out / "dpe" / f"dept-{dept}.parquet"
        report = prepare_dpe.prepare(rows, dept, destination)
        typer.echo(f"{dept} : {json.dumps(report, ensure_ascii=False)}")
        typer.secho(f"  -> {destination}", fg=typer.colors.GREEN)


def _prepare_bdnb(entry: registry.Source, departments: list[str], out: Path) -> None:
    """L'archive fait ~39 Go pour une centaine de tables ; on n'en extrait que cinq."""
    csv_dir = WORK / "bdnb" / "csv"
    members = [f"./csv/{table}.csv" for table in prepare_bdnb.TABLES.values()]

    if not (csv_dir / f"{prepare_bdnb.TABLES['groupe']}.csv").exists():
        typer.echo(
            f"extraction de {len(members)} tables depuis l'archive BDNB (~40 Go a traverser).\n"
            "L'archive n'est pas indexable : il faut la lire en flux, mais la lecture\n"
            "s'arrete des que les tables voulues sont sorties."
        )

        def progression(lus: int, total: int | None) -> None:
            part = f" / {total / 1e9:.1f} Go" if total else ""
            typer.echo(f"\r  {lus / 1e9:6.2f} Go lus{part}", nl=False, err=True)

        found = fetch.extract_members(
            entry.url("csv_archive"), csv_dir, members,
            on_progress=progression,
            on_member=lambda nom: typer.secho(f"\r  extraite : {nom}", fg=typer.colors.GREEN),
            on_retry=lambda n, err: typer.secho(
                f"\r  tentative {n} interrompue ({err}) — reprise ; "
                "les tables deja extraites sont conservees",
                fg=typer.colors.YELLOW,
            ),
        )
        typer.echo("")
        typer.echo(f"  {len(found)} table(s) extraite(s)")

    destination = out / "bdnb" / ("france.parquet" if not departments else f"dept-{departments[0]}.parquet")
    report = prepare_bdnb.prepare(csv_dir, destination, departments=departments or None)
    typer.echo(f"bdnb : {json.dumps(report, ensure_ascii=False)}")
    typer.secho(f"  -> {destination}", fg=typer.colors.GREEN)


def _probe_source(entry: registry.Source) -> fetch.RemoteInfo:
    """Marque de version d'une source, quelle que soit sa forme.

    Un fichier expose un ETag ; une API expose une date de mise a jour. Les deux
    servent au meme usage : savoir si quelque chose a change depuis la derniere
    fois.
    """
    if "probe" in entry.urls:
        return fetch.probe(entry.url("probe"))
    if "api" in entry.urls:
        return fetch.probe_dataset(entry.url("api"))
    if "file" in entry.urls:
        return fetch.probe(entry.url("file"))
    raise KeyError("aucune URL sondable dans le registre")


@app.command()
def prepare(
    source: str = typer.Option(..., help="dvf, dpe ou bdnb."),
    departments: str = typer.Option(
        "all",
        help="Codes departement separes par des virgules, ou « all » pour tout le jeu (defaut).",
    ),
    years: str = typer.Option("2021,2022,2023,2024,2025", help="Annees DVF a consolider."),
    out: Path = typer.Option(Path("out"), help="Repertoire de sortie."),
    jobs: int = typer.Option(4, help="Departements traites en parallele."),
    force: bool = typer.Option(False, "--force", help="Refaire meme si le fichier existe deja."),
    historique: bool = typer.Option(
        True, "--historique/--sans-historique",
        help="Ajouter les annees anterieures a la fenetre glissante DVF (2018-2020, metropole).",
    ),
) -> None:
    """Telecharge et prepare une source, departement par departement.

    Sans --departments, tout le jeu est traite. La liste n'est pas codee en dur :
    elle est demandee a la source, qui sait seule ce qu'elle publie.
    """
    catalogue = registry.registry()
    api_url = catalogue["dpe"].urls.get("api") if source == "dpe" else None
    depts = depts_module.resolve(departments, source, api_url)

    if departments.lower() in ("all", "tous"):
        typer.echo(f"{len(depts)} territoire(s) a traiter : {', '.join(depts[:8])}…")

    if source == "dpe":
        _prepare_dpe(catalogue["dpe"], depts, out, jobs, force)
        return
    if source == "bdnb":
        _prepare_bdnb(catalogue["bdnb"], depts, out)
        return
    if source != "dvf":
        typer.secho(f"source « {source} » pas encore implementee", fg=typer.colors.YELLOW)
        raise typer.Exit(2)

    entry = catalogue["dvf"]
    year_list = [y.strip() for y in years.split(",") if y.strip()]
    archive = catalogue.get("dvf_historique") if historique else None
    echecs = _run_parallel(
        depts, jobs, lambda d: _prepare_dvf_one(entry, d, year_list, out, force, archive)
    )
    if echecs and echecs == len(depts):
        # Tous en echec : ce n'est pas une source capricieuse, c'est une panne.
        raise typer.Exit(1)


@app.command()
def run(
    millesime: str = typer.Option(None, help="Millesime a publier. Sans lui, on s'arrete apres la verification."),
    sources: str = typer.Option("dvf,dpe", help="Sources a traiter, separees par des virgules."),
    out: Path = typer.Option(Path("out"), help="Repertoire de sortie."),
    jobs: int = typer.Option(6, help="Territoires traites en parallele."),
    force: bool = typer.Option(False, "--force", help="Refaire meme ce qui existe deja."),
) -> None:
    """Enchaine tout : detection, preparation de chaque source, verification, publication.

    Reprenable : un territoire deja produit est saute, sauf avec --force. Une
    execution interrompue se relance donc sans tout refaire — ce qui compte
    quand la chaine complete demande plus d'une heure.
    """
    demandees = [s.strip() for s in sources.split(",") if s.strip()]

    typer.secho("\n1. Nouvelles donnees ?", bold=True)
    try:
        check.callback(source=None, state_file=STATE_FILE)
    except SystemExit:
        pass
    except Exception as error:  # noqa: BLE001
        typer.secho(f"  detection impossible : {error}", fg=typer.colors.YELLOW)

    for source in demandees:
        typer.secho(f"\n2. Preparation — {source}", bold=True)
        try:
            prepare(source=source, departments="all", years="2021,2022,2023,2024,2025",
                    out=out, jobs=jobs, force=force)
        except SystemExit as stop:
            if stop.code:
                # Avaler cet echec menerait a verifier ce qui existe deja et a
                # conclure que tout va bien, alors que la source n'a rien produit.
                typer.secho(
                    f"\nPreparation de « {source} » en echec. Chaine interrompue, rien n'a ete publie.",
                    fg=typer.colors.RED, bold=True,
                )
                raise

    typer.secho("\n3. Controles qualite", bold=True)
    try:
        verify(out=out, baseline=None, expect=",".join(demandees))
    except SystemExit as stop:
        if stop.code:
            typer.secho(
                "\nChaine interrompue : un controle bloque. Rien n'a ete publie.",
                fg=typer.colors.RED, bold=True,
            )
            raise

    if not millesime:
        typer.secho("\nPret a publier. Relancer avec --millesime pour envoyer.", fg=typer.colors.YELLOW)
        return

    typer.secho(f"\n4. Publication — {millesime}", bold=True)
    publish(millesime=millesime, out=out, source=None,
            repo="cldt-fr/dvf-bdnb-ademe", dry_run=False, skip_checks=True)


@app.command()
def join(
    departments: str = typer.Option("all", help="Codes departement, ou « all » (defaut)."),
    out: Path = typer.Option(Path("out"), help="Repertoire contenant les jeux prepares."),
    jobs: int = typer.Option(4, help="Departements traites en parallele."),
    force: bool = typer.Option(False, "--force", help="Refaire meme si le fichier existe deja."),
) -> None:
    """Produit LE fichier qui contient tout : une ligne par vente, avec son
    batiment et son diagnostic energetique.

    Exige que les jeux DVF, BDNB et DPE aient deja ete prepares. La BDNB est le
    pont : sans elle, aucune vente ne peut atteindre un diagnostic, faute de cle
    commune entre une parcelle et un DPE.
    """
    if not (out / "dvf").exists():
        typer.secho("le jeu DVF doit etre prepare d'abord.", fg=typer.colors.RED)
        raise typer.Exit(2)

    depts = (
        [d.strip().upper() for d in departments.split(",") if d.strip()]
        if departments.lower() not in ("all", "tous")
        else sorted(p.stem.removeprefix("dept-") for p in (out / "dvf").glob("*.parquet"))
    )

    manquantes = [s for s in ("bdnb", "dpe") if not (out / s).exists()]
    if manquantes:
        typer.secho(
            f"sources absentes : {', '.join(manquantes)}. Le jeu sera produit avec "
            "les colonnes correspondantes vides.",
            fg=typer.colors.YELLOW,
        )

    _run_parallel(depts, jobs, lambda d: _join_one(d, out, force))


def _join_one(dept: str, out: Path, force: bool) -> str:
    destination = out / "joint" / f"dept-{dept}.parquet"
    if destination.exists() and not force:
        return "deja produit (--force pour refaire)"

    bdnb_path = out / "bdnb" / f"dept-{dept}.parquet"
    if not bdnb_path.exists():
        # La BDNB peut avoir ete preparee France entiere plutot que par departement.
        france = out / "bdnb" / "france.parquet"
        bdnb_path = france if france.exists() else bdnb_path

    report = prepare_joint.prepare(
        out / "dvf" / f"dept-{dept}.parquet",
        destination,
        bdnb_parquet=bdnb_path if bdnb_path.exists() else None,
        dpe_parquet=out / "dpe" / f"dept-{dept}.parquet",
    )
    return (
        f"{report['ventes']} ventes, {report['part_avec_batiment']} % avec batiment, "
        f"{report['part_avec_classe_energie']} % avec classe energie"
    )


@app.command()
def verify(
    out: Path = typer.Option(Path("out"), help="Repertoire contenant les jeux prepares."),
    baseline: Path = typer.Option(None, help="Manifeste du millesime precedent, pour la comparaison."),
    expect: str = typer.Option(None, help="Sources qui DOIVENT avoir produit des donnees."),
) -> None:
    """Passe les jeux prepares au crible avant publication.

    Code de sortie 1 si un controle bloque : de quoi arreter une chaine
    automatique avant qu'elle ne publie un jeu faux.
    """
    reference = {}
    if baseline and baseline.exists():
        reference = quality.baseline_from_manifest(json.loads(baseline.read_text(encoding="utf-8")))

    findings = []

    # Une source qui n'a rien produit est invisible d'un controle qui ne regarde
    # que les fichiers existants : la chaine annoncerait « tout va bien » sur un
    # echec total. On exige donc explicitement ce qui doit etre la.
    produites = {source for _, source, _ in _iter_datasets(out)}
    for attendue in [s.strip() for s in (expect or "").split(",") if s.strip()]:
        if attendue not in produites:
            findings.append(quality.Finding(
                quality.Level.FAIL, "presence",
                f"{attendue} : aucune donnee produite, alors que la source etait demandee",
            ))

    for parquet, source, dept in _iter_datasets(out):
        rapport = quality.check_dataset(
            parquet, source, dept, baseline=reference.get(f"{source}/{dept}")
        )
        findings.extend(rapport.findings)

    if not findings:
        typer.secho("aucun jeu a verifier", fg=typer.colors.RED)
        raise typer.Exit(2)

    bloquants = 0
    for finding in findings:
        couleur = {
            quality.Level.OK: typer.colors.GREEN,
            quality.Level.WARN: typer.colors.YELLOW,
            quality.Level.FAIL: typer.colors.RED,
        }[finding.level]
        typer.secho(str(finding), fg=couleur)
        bloquants += finding.level is quality.Level.FAIL

    typer.echo("")
    if bloquants:
        typer.secho(
            f"{bloquants} controle(s) bloquant(s) — publication refusee. "
            "Mieux vaut ne rien publier qu'un jeu faux.",
            fg=typer.colors.RED, bold=True,
        )
        raise typer.Exit(1)

    typer.secho("tous les controles passent.", fg=typer.colors.GREEN, bold=True)


def _iter_datasets(out: Path):
    """Parcourt les Parquet produits, en deduisant source et departement."""
    for directory in sorted(p for p in out.iterdir() if p.is_dir() and p.name != "schema"):
        for parquet in sorted(directory.glob("*.parquet")):
            dept = parquet.stem.removeprefix("dept-")
            yield parquet, directory.name, dept


@app.command()
def publish(
    millesime: str = typer.Option(..., help="Identifiant du millesime, ex. 2026-02-a."),
    out: Path = typer.Option(Path("out"), help="Repertoire contenant les jeux prepares."),
    source: str = typer.Option(None, help="Ne publier qu'une source."),
    repo: str = typer.Option("cldt-fr/dvf-bdnb-ademe", help="Depot GitHub cible."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Tout preparer sans rien envoyer."),
    skip_checks: bool = typer.Option(False, "--skip-checks", help="Publier malgre un controle bloquant."),
) -> None:
    """Decline les jeux prepares en CSV + DDL, et les publie en Release."""
    if not out.exists():
        typer.secho(f"rien a publier : {out} n'existe pas", fg=typer.colors.RED)
        raise typer.Exit(2)

    # Les controles precedent la publication, jamais l'inverse : une chaine
    # automatique doit s'arreter AVANT d'avoir publie, pas apres.
    if not skip_checks:
        bloquants = []
        for parquet, src, dept in _iter_datasets(out):
            if source and src != source:
                continue
            bloquants.extend(quality.check_dataset(parquet, src, dept).blocking)
        if bloquants:
            for finding in bloquants:
                typer.secho(str(finding), fg=typer.colors.RED)
            typer.secho(
                "publication refusee. Relancer `verify` pour le detail, ou forcer "
                "avec --skip-checks en connaissance de cause.",
                fg=typer.colors.RED, bold=True,
            )
            raise typer.Exit(1)

    sources = [source] if source else None
    bundles = publisher.build(out, millesime, sources)

    if not bundles:
        typer.secho("aucun jeu prepare trouve", fg=typer.colors.RED)
        raise typer.Exit(2)

    manifest_path = publisher.write_manifest(bundles, millesime, out / "manifest.json")

    for bundle in bundles:
        parquets = [a for a in bundle.assets if a.path.suffix == ".parquet"]
        total = sum(a.rows or 0 for a in parquets)
        typer.echo(f"{bundle.source:5} : {len(parquets)} departement(s), {total:,} lignes".replace(",", " "))
        typer.echo(f"        DDL : {bundle.ddl}")

    typer.echo(f"manifeste : {manifest_path}")

    if dry_run:
        typer.secho("--dry-run : rien n'a ete envoye.", fg=typer.colors.YELLOW)
        return

    _upload(bundles, manifest_path, millesime, repo)


def _upload(bundles: list, manifest_path: Path, millesime: str, repo: str) -> None:
    """Cree la Release et y depose les fichiers.

    On passe par `gh` plutot que par l'API : l'authentification, la reprise des
    envois volumineux et les limites de taille y sont deja gerees.
    """
    import shutil
    import subprocess

    if shutil.which("gh") is None:
        typer.secho(
            "gh est introuvable. Installer GitHub CLI, ou reprendre les fichiers "
            f"de {manifest_path.parent} a la main.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(2)

    files = [str(manifest_path)]
    for bundle in bundles:
        files += [str(a.path) for a in bundle.assets]
        if bundle.ddl:
            files.append(str(bundle.ddl))

    existing = subprocess.run(
        ["gh", "release", "view", millesime, "--repo", repo],
        capture_output=True, text=True,
    )
    if existing.returncode != 0:
        typer.echo(f"creation de la Release {millesime}…")
        subprocess.run(
            ["gh", "release", "create", millesime, "--repo", repo,
             "--title", f"Millesime {millesime}",
             "--notes", _release_notes(bundles, millesime)],
            check=True,
        )

    typer.echo(f"envoi de {len(files)} fichier(s)…")
    subprocess.run(
        ["gh", "release", "upload", millesime, *files, "--repo", repo, "--clobber"],
        check=True,
    )
    typer.secho(f"publie : https://github.com/{repo}/releases/tag/{millesime}", fg=typer.colors.GREEN)


def _release_notes(bundles: list, millesime: str) -> str:
    lignes = [
        f"Jeux prepares du millesime **{millesime}**.",
        "",
        "| Source | Departements | Lignes |",
        "|--------|--------------|--------|",
    ]
    for bundle in bundles:
        parquets = [a for a in bundle.assets if a.path.suffix == ".parquet"]
        total = sum(a.rows or 0 for a in parquets)
        lignes.append(f"| {bundle.source} | {len(parquets)} | {total:,} |".replace(",", " "))
    lignes += [
        "",
        "Chaque source est livree en Parquet (analyse directe) et en CSV compresse",
        "accompagne de son DDL PostgreSQL (chargement par COPY).",
        "",
        "`manifest.json` porte les empreintes SHA-256 : verifier un telechargement",
        "avant de s'en servir, la source amont n'etant pas toujours fiable.",
        "",
        "Donnees sous Licence Ouverte 2.0 (Etalab) — DGFiP, CSTB, ADEME.",
    ]
    return "\n".join(lignes)


if __name__ == "__main__":
    app()
