"""Adım 0 (opsiyonel) — LCL ile BİREBİR aynı formatta sentetik ham veri üretir.

Ne zaman kullanılır?
  * Gerçek veri indirilmeden önce pipeline'ı test etmek (CI, hızlı deneme).
  * Gerçek veriden daha büyük ölçekte deney gerekirse: bkz. s03_make_subsets --upscale.

Üretilen veri, gerçek LCL'deki yapıları ve kusurları taklit eder:
  * yarım saatlik okumalar, hanelerin farklı tarihlerde katılıp ayrılması
  * sabah/akşam pikleri, hafta sonu kayması, kışın yüksek tüketim (mevsimsellik)
  * ACORN demografik grubu ve Std/ToU tarife ayrımı
  * kusurlar: "Null" metin değerleri, çok günlük boşluklar (gap), tekrar eden
    satırlar, yarım saat ızgarasına oturmayan zaman damgaları, uç değerler

UYARI: Rapordaki BULGULAR gerçek veriyle üretilmelidir. Sentetik veri yalnızca
kodun doğruluğunu sınamak içindir.

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s00_make_synthetic_raw --households 220
"""
import argparse

import numpy as np
import pandas as pd

from src.common.config import RAW_DIR, SEED, ensure_dirs

START = pd.Timestamp("2011-11-23 09:00")
END = pd.Timestamp("2014-02-28 00:00")

ACORN = {  # grup -> (ACORN harfleri, olasılık, tüketim çarpanı)
    "Affluent": (list("ABCDE"), 0.38, 1.20),
    "Comfortable": (list("FGHIJ"), 0.27, 1.00),
    "Adversity": (list("KLMNOPQ"), 0.33, 0.85),
    "ACORN-U": (["U"], 0.02, 1.00),
}


def _bump(h, center, width):
    d = np.minimum(np.abs(h - center), 24 - np.abs(h - center))
    return np.exp(-0.5 * (d / width) ** 2)


def make_household(i: int, rng: np.random.Generator) -> pd.DataFrame:
    groups = list(ACORN)
    g = rng.choice(groups, p=[ACORN[k][1] for k in groups])
    letters, _, mult = ACORN[g]
    acorn = "ACORN-" + rng.choice(letters) if g != "ACORN-U" else "ACORN-U"
    tariff = "ToU" if rng.random() < 0.2 else "Std"

    # katılım / ayrılma tarihleri (gerçek veride haneler kademeli katılır)
    start = START + pd.Timedelta(days=int(rng.uniform(0, 420)))
    start = start.floor("30min")
    end = END if rng.random() > 0.15 else END - pd.Timedelta(days=int(rng.uniform(30, 400)))
    ts = pd.date_range(start, end, freq="30min")

    h = ts.hour.values + ts.minute.values / 60
    wknd = ts.dayofweek.values >= 5
    doy = ts.dayofyear.values

    base = rng.lognormal(np.log(0.09), 0.4) * mult
    morning = rng.uniform(0.05, 0.25) * mult
    evening = rng.uniform(0.15, 0.6) * mult
    m_center = np.where(wknd, rng.uniform(8.5, 10.5), rng.uniform(6.5, 8.0))
    shape = (base
             + morning * _bump(h, m_center, 1.0)
             + evening * _bump(h, rng.uniform(18, 20.5), 1.6)
             + np.where(wknd, 0.08 * mult * _bump(h, 13, 2.5), 0.0))
    season = 1 + rng.uniform(0.15, 0.40) * np.cos(2 * np.pi * (doy - 15) / 365.25)
    kwh = shape * season * rng.lognormal(0, 0.35, len(ts))
    spikes = rng.random(len(ts)) < 0.00005
    kwh[spikes] *= rng.uniform(8, 25, spikes.sum())          # uç değerler

    df = pd.DataFrame({
        "LCLid": f"MAC{100000 + i:06d}",
        "stdorToU": tariff,
        "DateTime": ts,
        "kwh": np.round(kwh, 3),
        "Acorn": acorn,
        "Acorn_grouped": g,
    })

    # --- kusurlar ---
    if rng.random() < 0.35:                                   # çok günlük veri boşlukları
        for _ in range(rng.integers(1, 4)):
            a = rng.integers(0, max(1, len(df) - 500))
            df = df.drop(df.index[a:a + int(rng.integers(6, 48 * 10))], errors="ignore")
    df["kwh"] = df["kwh"].map(lambda v: f"{v:.3f}")
    null_mask = rng.random(len(df)) < 0.0002
    df.loc[null_mask, "kwh"] = "Null"
    dup = df.sample(frac=0.0001, random_state=int(rng.integers(1e9)))
    df = pd.concat([df, dup]).sort_values("DateTime", kind="stable")
    ts_str = df["DateTime"].dt.strftime("%Y-%m-%d %H:%M:%S") + ".0000000"
    off = rng.random(len(df)) < 0.00005                      # ızgara dışı zaman damgası
    ts_str = ts_str.where(~off, (df["DateTime"] + pd.Timedelta(seconds=1))
                          .dt.strftime("%Y-%m-%d %H:%M:%S") + ".0000000")
    df["DateTime"] = ts_str
    return df.rename(columns={"kwh": "KWH/hh (per half hour) "})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--households", type=int, default=220)
    ap.add_argument("--per-file", type=int, default=40, help="dosya başına hane")
    args = ap.parse_args()
    ensure_dirs()
    out = RAW_DIR / "synthetic_csv"   # gerçek veriyle karışmasın diye ayrı klasör
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    total, part, buf = 0, 0, []
    for i in range(args.households):
        buf.append(make_household(i, rng))
        if len(buf) == args.per_file or i == args.households - 1:
            df = pd.concat(buf)
            f = out / f"synthetic_LCL_part{part:03d}.csv"
            df.to_csv(f, index=False)
            total += len(df)
            print(f"[yaz] {f.name}: {len(df):,} satır (toplam {total:,})")
            part, buf = part + 1, []
    print(f"Bitti: {args.households} hane, {total:,} satır -> {out}")


if __name__ == "__main__":
    main()
