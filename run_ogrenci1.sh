#!/usr/bin/env bash
# Öğrenci 1 pipeline'ı uçtan uca çalıştırır.
#   ./run_ogrenci1.sh real        -> gerçek LCL verisi (partitioned, ilk 8 dosya ≈ 8M satır)
#   ./run_ogrenci1.sh real-full   -> gerçek LCL verisinin tamamı (~167M satır, ~10 GB)
#   ./run_ogrenci1.sh synthetic   -> internet olmadan test (LCL formatında sentetik veri)
set -euo pipefail
cd "$(dirname "$0")"
MODE="${1:-real}"

INPUT="data/raw/lcl_csv"
case "$MODE" in
  synthetic) python -m src.ogrenci1_veri_hazirlama.s00_make_synthetic_raw --households 220
             INPUT="data/raw/synthetic_csv" ;;
  real)      python -m src.ogrenci1_veri_hazirlama.s01_download --variant partitioned --max-files 8 ;;
  real-full) python -m src.ogrenci1_veri_hazirlama.s01_download --variant full ;;
  *) echo "kullanım: $0 [real|real-full|synthetic]"; exit 1 ;;
esac

python -m src.ogrenci1_veri_hazirlama.s02_prepare --input "$INPUT"
python -m src.ogrenci1_veri_hazirlama.s03_make_subsets
python -m src.ogrenci1_veri_hazirlama.s04_ts_structure
echo "Bitti. Rapor: docs/ogrenci1_bulgular.md  | Metrikler: metrics/"
