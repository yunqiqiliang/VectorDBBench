from os import walk
from pathlib import Path
from typing import List


def _resolve_glob(path: str) -> List[Path]:
    # TODO(guantao.gao) support Windows path
    if path.startswith("/"):
        return [p.absolute() for p in Path("/").glob(path[1:])]
    else:
        return list(Path(".").glob(path))


def _resolve_dir(path: str) -> List[Path]:
    res = []
    for root, _, files in walk(path):
        root_path = Path(root)
        res += (root_path / file for file in files)
    return res


def resolve_local_path(path: str) -> List[Path]:
    if "*" in path:
        return _resolve_glob(path)
    if path.endswith("/"):
        return _resolve_dir(path)
    return [Path(path)]
