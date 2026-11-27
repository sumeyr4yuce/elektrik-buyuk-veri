"""Adım 1 — Low Carbon London (LCL) akıllı sayaç verisini indirir.

Kaynak: London Datastore / UK Power Networks, "SmartMeter Energy Consumption
Data in London Households". 5.567 hane, Kasım 2011 – Şubat 2014, yarım saatlik
okumalar, ~167 milyon satır, açılmış hali ~10 GB.

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s01_download --variant full
    python -m src.ogrenci1_veri_hazirlama.s01_download --variant partitioned --max-files 6

--variant partitioned: aynı veri 168 adet ~1M satırlık CSV'ye bölünmüş haldedir.
Colab / dizüstü bilgisayarda --max-files ile sadece ilk N dosyayı açmak
(ör. 6 dosya ≈ 6M satır) 5M deneyine yeter ve diski doldurmaz.
"""
import argparse
import shutil
import urllib.request
import zipfile
from pathlib import Path

from src.common.config import RAW_DIR, ensure_dirs

URLS = {
    "full": "https://data.london.gov.uk/download/vqm0d/3527bf39-d93e-4071-8451-df2ade1ea4f2/LCL-FullData.zip",
    "partitioned": "https://data.london.gov.uk/download/vqm0d/04feba67-f1a3-4563-98d0-f3071e3d56d1/Partitioned%20LCL%20Data.zip",
    "tariffs": "https://data.london.gov.uk/download/vqm0d/14855047-44c2-4856-8a48-e5649200e6ce/Tariffs.xlsx",
}


def download(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[atla] zaten var: {dest}")
        return dest
    print(f"[indir] {url}\n     -> {dest}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        while chunk := r.read(8 << 20):
            f.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r     {done / total:6.1%}  ({done >> 20} / {total >> 20} MB)", end="")
    print()
    tmp.rename(dest)
    return dest


def extract(zip_path: Path, out_dir: Path, max_files: int | None) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        members = sorted(m for m in z.namelist() if m.lower().endswith(".csv"))
        if max_files:
            members = members[:max_files]
        written = []
        for m in members:
            target = out_dir / Path(m).name          # klasör yapısını düzleştir
            if not target.exists():
                with z.open(m) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst, 16 << 20)
            written.append(target)
    print(f"[aç] {len(written)} CSV -> {out_dir}")
    return written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", choices=["full", "partitioned"], default="partitioned")
    ap.add_argument("--max-files", type=int, default=None,
                    help="partitioned için açılacak dosya sayısı (boş = hepsi)")
    ap.add_argument("--with-tariffs", action="store_true")
    args = ap.parse_args()

    ensure_dirs()
    zpath = download(URLS[args.variant], RAW_DIR / f"lcl_{args.variant}.zip")
    extract(zpath, RAW_DIR / "lcl_csv", args.max_files)
    if args.with_tariffs:
        download(URLS["tariffs"], RAW_DIR / "Tariffs.xlsx")


if __name__ == "__main__":
    main()
