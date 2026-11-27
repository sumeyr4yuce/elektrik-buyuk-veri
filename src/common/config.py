"""Tüm ekip için ortak yollar ve deney parametreleri.

Diğer öğrenciler (2-5) veri yollarını buradan okur; böylece herkes aynı
veri sürümü üzerinde çalışır.
"""
from pathlib import Path
import os

ROOT = Path(os.environ.get("EBV_ROOT", Path(__file__).resolve().parents[2]))

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"                 # indirilen ham LCL CSV'leri
PROCESSED_DIR = DATA_DIR / "processed"     # Öğrenci 1 çıktısı: canonical Parquet
SUBSETS_DIR = DATA_DIR / "subsets"         # 100k / 1m / 5m deney kümeleri
METRICS_DIR = ROOT / "metrics"             # JSON ölçümler (süre, boyut, istatistik)
FIG_DIR = ROOT / "docs" / "figures"
DOCS_DIR = ROOT / "docs"

CANONICAL_PATH = PROCESSED_DIR / "readings.parquet"   # yarım saatlik okumalar
METERS_PATH = PROCESSED_DIR / "meters.parquet"        # abone (sayaç) meta tablosu

# Deney boyutları (kayıt sayısı). Ödev: "en az 3 farklı veri büyüklüğü".
SUBSET_SIZES = {
    "100k": 100_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
}

SEED = 42
READING_INTERVAL_MIN = 30          # LCL: yarım saatlik okumalar
SLOTS_PER_DAY = 48
SLOTS_PER_WEEK = 48 * 7            # 336


def subset_path(name: str, fmt: str) -> Path:
    """subset_path('1m', 'parquet') -> data/subsets/1m/parquet"""
    return SUBSETS_DIR / name / fmt


def ensure_dirs() -> None:
    for d in (RAW_DIR, PROCESSED_DIR, SUBSETS_DIR, METRICS_DIR, FIG_DIR):
        d.mkdir(parents=True, exist_ok=True)
