"""Ölçüm yardımcıları: süre, disk boyutu, JSON metrik kaydı."""
import json
import time
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def timer(store: dict, key: str):
    t0 = time.perf_counter()
    yield
    store[key] = round(time.perf_counter() - t0, 3)


def dir_size_bytes(path: Path) -> int:
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    # Spark'ın _SUCCESS / .crc yan dosyalarını sayma
    return sum(p.stat().st_size for p in path.rglob("*")
               if p.is_file() and not p.name.startswith(("_", ".")))


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def save_metrics(obj: dict, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
