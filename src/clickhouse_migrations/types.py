import hashlib
import os
from collections import namedtuple
from pathlib import Path
from typing import List

Migration = namedtuple("Migration", ["version", "md5", "script"])


class MigrationStorage:
    def __init__(self, storage_dir: Path):
        self.storage_dir: Path = storage_dir
        self.migrations: List[Migration] = []

        for f in os.scandir(self.storage_dir):
            if f.name.endswith(".sql"):
                full_path: Path = self.storage_dir / f.name

                migration = Migration(
                    version=int(f.name.split("_")[0].replace("V", "")),
                    script=str(full_path),
                    md5=hashlib.md5(full_path.read_bytes()).hexdigest(),
                )

                self.migrations.append(migration)

    def count(self) -> int:
        return len(self.migrations)
