"""État des millésimes déjà traités.

Aucun producteur ne publie de notification : la détection se fait en comparant
l'empreinte d'une source à ce qu'on en connaît. Cet état est donc la mémoire du
pipeline.

Il n'est **pas versionné** : un fichier d'état commité se salit à chaque essai
local, entre en conflit dès que deux exécutions se croisent, et mélange les
essais d'un développeur avec ce qu'a réellement publié la chaîne.

À terme, la mémoire de référence sera le manifeste de la dernière Release
publiée : il dit ce qui a été produit, il est déjà versionné par nature, et il
ne peut pas diverger de la réalité. Ce fichier local n'est qu'un relais en
attendant la phase 5.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class SourceState:
    """Ce qu'on sait de la derniere version vue d'une source."""

    etag: str | None = None
    last_modified: str | None = None
    size: int | None = None
    seen_at: str | None = None
    prepared_at: str | None = None

    def differs_from(self, etag: str | None, last_modified: str | None, size: int | None) -> bool:
        """Une de ces trois marques suffit a conclure a une nouvelle version.

        On ne se contente pas de la taille : deux millesimes peuvent peser
        exactement pareil, et l'ETag change des que le contenu change.
        """
        if self.etag is None and self.size is None:
            return True
        if etag is not None and self.etag is not None:
            return etag != self.etag
        if last_modified is not None and self.last_modified is not None:
            return last_modified != self.last_modified
        return size != self.size


class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, dict] = {}
        if path.exists():
            self._data = json.loads(path.read_text(encoding="utf-8"))

    def get(self, source: str) -> SourceState:
        return SourceState(**self._data.get(source, {}))

    def set(self, source: str, state: SourceState) -> None:
        state.seen_at = datetime.now(UTC).isoformat(timespec="seconds")
        self._data[source] = {k: v for k, v in asdict(state).items() if v is not None}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
