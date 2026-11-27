"""Adım 4 — Zaman serisi yapısının incelenmesi (Öğrenci 1'in analitik katkısı).

Tüm ağır hesaplar Spark'ta yapılır; Spark'tan sürücüye yalnızca küçük, özet
tablolar (en fazla on binlerce satır) toplanıp grafiklenir.

Cevaplanan sorular
  S1. Seri düzenli mi?  -> ardışık okumalar arası süre (WINDOW lag), boşluk,
      tekrar, ızgara dışı zaman damgası
  S2. Aboneler ne kadar eksiksiz?  -> abone bazlı kapsama oranı
  S3. Panel zamanla nasıl değişiyor?  -> gün başına aktif abone sayısı
  S4. Hangi periyotlar baskın?  -> gün içi profil, hafta içi/sonu, ay (mevsim),
      gün×saat ısı haritası, toplam serinin otokorelasyonu (lag 48 = 1 gün,
      lag 336 = 1 hafta)
  S5. Değer dağılımı nasıl?  -> çarpıklık, sıfır oranı, uç kuantiller
  S6. Demografik gruplar farklı mı?  -> ACORN grubuna ve tarifeye göre profil

Çıktılar: docs/figures/ogr1_*.png, metrics/s04_ts_structure.json,
          docs/ogrenci1_bulgular.md (sayılar otomatik doldurulur)

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s04_ts_structure              # tüm canonical veri
    python -m src.ogrenci1_veri_hazirlama.s04_ts_structure --subset 1m
"""
import argparse
import json
from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.config import (CANONICAL_PATH, DOCS_DIR, FIG_DIR, METRICS_DIR,
                               SLOTS_PER_DAY, SLOTS_PER_WEEK, ensure_dirs, subset_path)
from src.common.spark_session import get_spark
from src.common.utils import save_metrics, timer
from src.ogrenci1_veri_hazirlama import plot_style
from src.ogrenci1_veri_hazirlama.plot_style import INK2, SERIES, SEQ_CMAP, plt

DOW_TR = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]
AY_TR = ["Oca", "Şub", "Mar", "Nis", "May", "Haz", "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara"]


def fig_path(name):
    return FIG_DIR / f"ogr1_{name}.png"


# --------------------------------------------------------------------------- S1
def interval_analysis(df):
    """Her abone içinde ts'ye göre sıralayıp bir önceki okumayla farkı al (window lag)."""
    w = Window.partitionBy("meter_id").orderBy("ts")
    d = (df.select("meter_id", "ts")
           .withColumn("prev", F.lag("ts").over(w))
           .withColumn("dmin", (F.unix_timestamp("ts") - F.unix_timestamp("prev")) / 60.0)
           .filter(F.col("prev").isNotNull()))
    bucket = (F.when(F.col("dmin") == 0, "0 (tekrar)")
               .when(F.col("dmin") % 30 != 0, "ızgara dışı")
               .when(F.col("dmin") == 30, "30 dk (düzenli)")
               .when(F.col("dmin") <= 120, "1-2 saat boşluk")
               .when(F.col("dmin") <= 1440, "2-24 saat boşluk")
               .otherwise("> 1 gün boşluk"))
    buckets = {r["b"]: r["n"] for r in d.groupBy(bucket.alias("b")).agg(F.count("*").alias("n")).collect()}
    gaps = d.filter((F.col("dmin") > 30) & (F.col("dmin") % 30 == 0))   # ızgara dışı hariç
    g = gaps.agg(F.count("*").alias("n_gaps"),
                 F.max("dmin").alias("max_gap_min"),
                 F.expr("percentile_approx(dmin, 0.5)").alias("median_gap_min"),
                 F.sum(F.col("dmin") / 30 - 1).alias("missing_slots_in_gaps")).first().asDict()
    off_grid_ts = df.filter((F.second("ts") != 0) | ~F.minute("ts").isin(0, 30)).count()
    return {"interval_buckets": buckets, **g, "off_grid_timestamps": off_grid_ts}


# --------------------------------------------------------------------------- S2
def coverage(df):
    per = (df.groupBy("meter_id")
             .agg(F.min("ts").alias("a"), F.max("ts").alias("b"),
                  F.countDistinct("ts").alias("n_ts"), F.count("kwh").alias("n_valid"))
             .withColumn("expected", (F.unix_timestamp("b") - F.unix_timestamp("a")) / 1800 + 1)
             .withColumn("coverage", F.least(F.col("n_valid") / F.col("expected"), F.lit(1.0)))
             .withColumn("days_active", (F.unix_timestamp("b") - F.unix_timestamp("a")) / 86400)
             .toPandas())
    q = per["coverage"].quantile([0.01, 0.05, 0.25, 0.5]).round(4).to_dict()
    res = {"coverage_quantiles": {str(k): v for k, v in q.items()},
           "coverage_mean": round(per["coverage"].mean(), 4),
           "meters_cov_lt_90": int((per["coverage"] < 0.9).sum()),
           "meters_cov_ge_99": int((per["coverage"] >= 0.99).sum()),
           "days_active_median": round(per["days_active"].median(), 1)}

    fig, ax = plt.subplots(1, 2, figsize=(11, 3.6))
    ax[0].hist(per["coverage"] * 100, bins=40, color=SERIES[0], edgecolor="white", linewidth=1)
    ax[0].set(title="Abone başına kapsama oranı", xlabel="Geçerli okuma / beklenen (%)", ylabel="Abone sayısı")
    ax[1].hist(per["days_active"], bins=40, color=SERIES[0], edgecolor="white", linewidth=1)
    ax[1].set(title="Abone başına aktif süre", xlabel="Gün (ilk–son okuma)", ylabel="Abone sayısı")
    fig.savefig(fig_path("kapsama")); plt.close(fig)
    return res


# --------------------------------------------------------------------------- S3
def active_meters(df):
    p = (df.filter(F.col("kwh").isNotNull())
           .groupBy("date").agg(F.countDistinct("meter_id").alias("aktif"),
                                F.sum("kwh").alias("toplam_kwh"))
           .orderBy("date").toPandas())
    p["date"] = pd.to_datetime(p["date"])
    fig, ax = plt.subplots(figsize=(11, 3.4))
    ax.plot(p["date"], p["aktif"], color=SERIES[0], linewidth=1.5)
    ax.set(title="Gün başına veri gönderen abone sayısı", ylabel="Aktif abone")
    fig.savefig(fig_path("aktif_abone")); plt.close(fig)
    full = p[p["aktif"] >= 0.9 * p["aktif"].max()]
    return {"active_max": int(p["aktif"].max()),
            "active_median": int(p["aktif"].median()),
            "stable_panel_from": str(full["date"].min().date()) if len(full) else None,
            "stable_panel_to": str(full["date"].max().date()) if len(full) else None}


# --------------------------------------------------------------------------- S4
def profiles(df):
    v = df.filter(F.col("kwh").isNotNull())

    # Gün içi profil (hafta içi / sonu)
    prof = (v.groupBy("is_weekend", "slot").agg(F.avg("kwh").alias("m"))
              .toPandas().pivot(index="slot", columns="is_weekend", values="m").sort_index())
    x = np.arange(SLOTS_PER_DAY) / 2
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(x, prof[False], label="Hafta içi", color=SERIES[0])
    ax.plot(x, prof[True], label="Hafta sonu", color=SERIES[1])
    ax.set(title="Ortalama gün içi yük profili", xlabel="Saat", ylabel="kWh / yarım saat",
           xticks=range(0, 25, 3), xlim=(0, 23.5))
    ax.legend()
    fig.savefig(fig_path("gun_ici_profil")); plt.close(fig)
    wd = prof[False]
    peak_slot = int(wd.idxmax())
    morning = slice(10, 25)                    # 05:00–12:00 arası sabah piki
    m_wd, m_we = int(wd.loc[morning].idxmax()), int(prof[True].loc[morning].idxmax())
    res = {"weekday_peak_time": f"{peak_slot // 2:02d}:{30 * (peak_slot % 2):02d}",
           "weekday_peak_kwh": round(float(wd.max()), 4),
           "weekday_min_time": f"{int(wd.idxmin()) // 2:02d}:{30 * (int(wd.idxmin()) % 2):02d}",
           "peak_to_trough_ratio": round(float(wd.max() / wd.min()), 2),
           "weekend_over_weekday_daily": round(float(prof[True].sum() / prof[False].sum()), 3),
           "morning_peak_weekday": f"{m_wd // 2:02d}:{30 * (m_wd % 2):02d}",
           "morning_peak_weekend": f"{m_we // 2:02d}:{30 * (m_we % 2):02d}",
           "morning_peak_shift_h": (m_we - m_wd) / 2}

    # Gün × saat ısı haritası
    hm = (v.groupBy("dow", "hour").agg(F.avg("kwh").alias("m")).toPandas()
            .pivot(index="dow", columns="hour", values="m").sort_index())
    fig, ax = plt.subplots(figsize=(11, 3.2))
    im = ax.imshow(hm.values, aspect="auto", cmap=SEQ_CMAP)
    ax.set(title="Haftanın günü × saat: ortalama tüketim", xlabel="Saat",
           yticks=range(7), yticklabels=DOW_TR, xticks=range(0, 24, 2))
    ax.grid(False)
    fig.colorbar(im, ax=ax, label="kWh / yarım saat", pad=0.01)
    fig.savefig(fig_path("isi_haritasi")); plt.close(fig)

    # Mevsimsellik: abone başına ortalama GÜNLÜK tüketim, aya göre
    daily = v.groupBy("meter_id", "date").agg(F.sum("kwh").alias("d"), F.count("*").alias("n")) \
             .filter(F.col("n") >= 44)          # neredeyse tam günler (≥ 22 saat)
    mon = (daily.withColumn("month", F.month("date"))
                .groupBy("month").agg(F.avg("d").alias("m"),
                                      F.expr("percentile_approx(d, 0.25)").alias("q1"),
                                      F.expr("percentile_approx(d, 0.75)").alias("q3"))
                .orderBy("month").toPandas())
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.fill_between(mon["month"], mon["q1"], mon["q3"], color=SERIES[0], alpha=0.15, linewidth=0,
                    label="Çeyrekler arası (Q1–Q3)")
    ax.plot(mon["month"], mon["m"], marker="o", markersize=6, color=SERIES[0], label="Ortalama")
    ax.set(title="Aylara göre hane başı günlük tüketim", ylabel="kWh / gün",
           xticks=range(1, 13), xticklabels=AY_TR)
    ax.legend()
    fig.savefig(fig_path("mevsimsellik")); plt.close(fig)
    res.update({"daily_kwh_mean": round(float(daily.agg(F.avg("d")).first()[0]), 3),
                "winter_summer_ratio": round(float(
                    mon.set_index("month").loc[[12, 1, 2], "m"].mean()
                    / mon.set_index("month").loc[[6, 7, 8], "m"].mean()), 3)
                if set([1, 2, 6, 7, 8, 12]) <= set(mon["month"]) else None})
    return res


def autocorrelation(df):
    """Tüm abonelerin ortalaması olan toplu seri üzerinde ACF (lag 1..672)."""
    s = (df.filter(F.col("kwh").isNotNull())
           .groupBy("ts").agg(F.avg("kwh").alias("m"), F.count("*").alias("n"))
           .orderBy("ts").toPandas().set_index("ts"))
    s = s[s["n"] >= max(3, 0.2 * s["n"].max())]["m"]          # çok az abonenin olduğu anları at
    s = s.asfreq("30min").interpolate(limit=4).dropna()
    x = s.values - s.values.mean()
    nlag = 2 * SLOTS_PER_WEEK
    n = len(x)
    f = np.fft.rfft(x, 2 ** int(np.ceil(np.log2(2 * n))))
    acf = np.fft.irfft(f * np.conj(f))[: nlag + 1]
    acf = acf / acf[0]
    fig, ax = plt.subplots(figsize=(10, 3.4))
    ax.vlines(np.arange(nlag + 1) / 48, 0, acf, color=SERIES[0], linewidth=1)
    for lag, lbl in ((48, "1 gün"), (336, "1 hafta")):
        ax.axvline(lag / 48, color=INK2, linestyle=":", linewidth=1)
        ax.annotate(f"{lbl}\nr={acf[lag]:.2f}", (lag / 48, acf[lag]), xytext=(6, 4),
                    textcoords="offset points", color=INK2, fontsize=9)
    ax.axhline(0, color=INK2, linewidth=0.8)
    ax.set(title="Toplam yük serisinin otokorelasyonu (ACF)", xlabel="Gecikme (gün)", ylabel="r",
           ylim=(min(acf.min(), 0) - 0.05, 1.3))
    fig.savefig(fig_path("acf")); plt.close(fig)
    return {"acf_lag1": round(float(acf[1]), 3), "acf_lag24h": round(float(acf[48]), 3),
            "acf_lag12h": round(float(acf[24]), 3), "acf_lag1w": round(float(acf[336]), 3),
            "acf_series_points": n}


# --------------------------------------------------------------------------- S5
def value_distribution(df):
    v = df.filter(F.col("kwh").isNotNull())
    st = v.agg(F.count("kwh").alias("n"), F.avg("kwh").alias("mean"), F.stddev("kwh").alias("std"),
               F.skewness("kwh").alias("skew"), F.kurtosis("kwh").alias("kurt"),
               F.min("kwh").alias("min"), F.max("kwh").alias("max"),
               F.avg((F.col("kwh") == 0).cast("int")).alias("zero_share"),
               F.avg((F.col("kwh") < 0).cast("int")).alias("neg_share"),
               F.expr("percentile_approx(kwh, array(0.01,0.25,0.5,0.75,0.99,0.999), 10000)").alias("q")
               ).first().asDict()
    qs = dict(zip(["p01", "p25", "p50", "p75", "p99", "p999"], st.pop("q")))
    # Histogram Spark'ta hesaplanır (RDD histogram) — veriyi sürücüye çekmeden.
    hi = qs["p999"] * 1.5
    edges = list(np.linspace(0, hi, 61))
    _, counts = v.select(F.least("kwh", F.lit(hi)).alias("k")).filter("k >= 0").rdd \
                 .map(lambda r: r[0]).histogram(edges)
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.bar(edges[:-1], counts, width=np.diff(edges), align="edge", color=SERIES[0],
           edgecolor="white", linewidth=0.6)
    ax.set_yscale("log")
    ax.set(title="Yarım saatlik tüketim dağılımı (log ölçek)", xlabel=f"kWh / yarım saat  (son çubuk: ≥ {hi:.2f}, kırpılmış)", ylabel="Okuma sayısı")
    fig.savefig(fig_path("dagilim")); plt.close(fig)
    out = {k: (round(float(v_), 5) if v_ is not None else None) for k, v_ in st.items()}
    out.update({k: round(float(v_), 4) for k, v_ in qs.items()})
    return out


# --------------------------------------------------------------------------- S6
def group_profiles(df):
    v = df.filter(F.col("kwh").isNotNull() & ~F.col("is_weekend"))
    res = {}
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.6), sharey=True)
    for ax, col, title in ((axes[0], "acorn_grouped", "ACORN grubuna göre"),
                           (axes[1], "tariff", "Tarifeye göre")):
        if df.filter(F.col(col).isNotNull()).limit(1).count() == 0:
            ax.text(0.5, 0.5, f"{col} bilgisi yok\n(s02b_enrich_households çalıştırın)",
                    ha="center", va="center", transform=ax.transAxes, color=INK2)
            ax.set(title=f"Hafta içi profil — {title}")
            res[col] = {}
            continue
        p = (v.filter(F.col(col).isNotNull()).groupBy(col, "slot").agg(F.avg("kwh").alias("m"))
               .toPandas().pivot(index="slot", columns=col, values="m").sort_index())
        sizes = (df.groupBy(col).agg(F.countDistinct("meter_id").alias("n")).toPandas()
                   .set_index(col)["n"].to_dict())
        order = sorted(p.columns, key=lambda c: -sizes.get(c, 0))
        for i, c in enumerate(order):
            ax.plot(np.arange(48) / 2, p[c], color=SERIES[i % len(SERIES)],
                    label=f"{c} (n={sizes.get(c, 0)})")
        ax.set(title=f"Hafta içi profil — {title}", xlabel="Saat", xticks=range(0, 25, 3), xlim=(0, 23.5))
        ax.legend(fontsize=8)
        res[col] = {str(c): {"meters": int(sizes.get(c, 0)),
                             "mean_kwh_hh": round(float(p[c].mean()), 4)} for c in order}
        # şekil benzerliği: günlük toplamına normalize edilmiş profiller arası en düşük korelasyon
        big = [c for c in order if sizes.get(c, 0) >= 10] or order
        norm = p[big] / p[big].sum()
        res[f"{col}_min_shape_corr"] = round(float(norm.corr().values.min()), 3)
        res[f"{col}_level_ratio_max_min"] = round(float(p[big].mean().max() / p[big].mean().min()), 2)
    axes[0].set_ylabel("kWh / yarım saat")
    fig.savefig(fig_path("grup_profilleri")); plt.close(fig)
    return res


# --------------------------------------------------------------------------- rapor
def overview(df):
    o = df.agg(F.count("*").alias("rows"), F.countDistinct("meter_id").alias("meters"),
               F.min("ts").alias("ts_min"), F.max("ts").alias("ts_max"),
               F.sum(F.col("kwh").isNull().cast("int")).alias("null_kwh")).first().asDict()
    o["duplicate_rows_meter_ts"] = o["rows"] - df.select("meter_id", "ts").distinct().count()
    o["null_kwh_pct"] = round(100 * o["null_kwh"] / o["rows"], 4)
    return o


def write_report(m, label):
    o, iv, cv, ac = m["overview"], m["intervals"], m["coverage"], m["active"]
    pr, acf, dist = m["profiles"], m["acf"], m["distribution"]
    b = iv["interval_buckets"]
    tot_int = sum(b.values()) or 1
    reg = 100 * b.get("30 dk (düzenli)", 0) / tot_int

    def yorum_acf():
        if acf["acf_lag24h"] > 0.5:
            s = (f"Toplam serinin 1 günlük gecikmedeki otokorelasyonu **r = {acf['acf_lag24h']}**; "
                 "bu güçlü bir günlük periyodiklik demektir. ")
        else:
            s = f"1 günlük gecikmede otokorelasyon r = {acf['acf_lag24h']} (zayıf/orta). "
        if acf["acf_lag1w"] >= acf["acf_lag24h"]:
            s += (f"1 haftalık gecikmede r = {acf['acf_lag1w']}, yani günlük değerden de yüksek: "
                  "hafta içi/sonu davranış farkı haftalık bir döngü ekliyor. Tahmin modelinde (Öğrenci 4) "
                  "hem `lag_48` hem `lag_336` öznitelik olarak kullanılmalı.")
        else:
            s += (f"1 haftalık gecikmede r = {acf['acf_lag1w']}. Haftalık etki günlükten zayıf; "
                  "yine de `lag_336` özniteliği denenmeli.")
        return s

    def yorum_grup():
        g = m["groups"]
        if "acorn_grouped_min_shape_corr" not in g:
            return ("⚠️ ACORN bilgisi bu veride yok (güncel LCL sürümünde kolon çıkarılmış). "
                    "`s02b_enrich_households` ile hane tablosundan eklenmeli.")
        r, c = g["acorn_grouped_level_ratio_max_min"], g["acorn_grouped_min_shape_corr"]
        s = f"ACORN grupları arasında ortalama seviye farkı en fazla **{r}×**. "
        if c >= 0.95:
            s += (f"Normalize edilmiş (günlük toplamına bölünmüş) profiller arasındaki en düşük korelasyon "
                  f"r = {c}: gruplar *seviyede* ayrışıyor, *şekilde* neredeyse aynı. Kullanıcı profili "
                  "sınıflandırmasında (Öğrenci 4) demografik grup tahmini yerine tüketim şekline dayalı "
                  "kümeleme (normalize 48 dilim) daha anlamlı olur.")
        else:
            s += (f"Normalize profiller arası en düşük korelasyon r = {c}: gruplar *şekil* olarak da "
                  "ayrışıyor; profil sınıflandırması (Öğrenci 4) için ACORN grubu anlamlı bir etiket olabilir.")
        return s

    md = f"""# Öğrenci 1 — Veri Hazırlama ve Zaman Serisi Yapısı: Bulgular

> Otomatik üretildi: {datetime.now():%Y-%m-%d %H:%M} · Girdi: **{label}**
> Bu dosyadaki sayılar `s04_ts_structure.py` tarafından doldurulur. Gerçek LCL verisiyle yeniden çalıştırın.

## 1. Veri kümesine genel bakış

| Ölçüt | Değer |
|---|---|
| Kayıt (satır) | {o['rows']:,} |
| Abone (sayaç) | {o['meters']:,} |
| Zaman aralığı | {o['ts_min']} → {o['ts_max']} |
| kWh değeri boş (NULL) | {o['null_kwh']:,} (%{o['null_kwh_pct']}) |
| Tekrarlanan (abone, zaman) satırı | {o['duplicate_rows_meter_ts']:,} |
| Yarım saat ızgarasına oturmayan zaman damgası | {iv['off_grid_timestamps']:,} |

## 2. Örnekleme düzenliliği (Spark window `lag`)

Her abonenin okumaları zamana göre sıralanıp bir önceki okumayla arasındaki süre hesaplandı.

| Ardışık okuma arası | Adet | Pay |
|---|---|---|
""" + "\n".join(f"| {k} | {v:,} | %{100 * v / tot_int:.3f} |" for k, v in sorted(b.items(), key=lambda kv: -kv[1])) + f"""

Ardışık okumaların **%{reg:.2f}**'i beklenen 30 dakikalık aralıkta. Toplam **{iv['n_gaps'] or 0:,}** boşluk
bulundu; medyan boşluk {(iv['median_gap_min'] or 0) / 60:.1f} saat, en uzun boşluk {(iv['max_gap_min'] or 0) / 1440:.1f} gün.
Boşluklardaki kayıp yarım saat sayısı ≈ {int(iv['missing_slots_in_gaps'] or 0):,}.
**Sonuç:** seri *nominal olarak* düzenli, ama abone düzeyinde eksik dilimler var. Bu yüzden
`lag`/`rolling` tabanlı her öznitelik, gerçek zaman farkına bakılarak hesaplanmalı
(satır sırasına değil). Tekrarlar ve ızgara dışı damgalar Öğrenci 2'ye devredildi.

## 3. Abone kapsaması ve panelin zamanla değişimi

- Ortalama kapsama: **%{100 * cv['coverage_mean']:.2f}**; medyan %{100 * cv['coverage_quantiles']['0.5']:.2f}, en kötü %5'lik dilim %{100 * cv['coverage_quantiles']['0.05']:.2f}.
- Kapsaması %90'ın altında olan abone: **{cv['meters_cov_lt_90']}**; %99 ve üstü: {cv['meters_cov_ge_99']}.
- Medyan aktif süre: {cv['days_active_median']} gün.
- Gün başına aktif abone en fazla {ac['active_max']:,}, medyan {ac['active_median']:,}.
  Aktif abone sayısının zirvenin %90'ının üstünde olduğu **dengeli panel dönemi: {ac['stable_panel_from']} → {ac['stable_panel_to']}**.

**Neden önemli:** Haneler projeye kademeli katıldığı için toplam tüketimin ham toplamı (sum) zamanla
yapay olarak artar. Pik talep analizinde (Öğrenci 5) **abone başına ortalama** veya dengeli panel dönemi
kullanılmalı; aksi halde "talep arttı" sonucu yalnızca abone sayısının artışını yansıtır.

![Kapsama](figures/ogr1_kapsama.png)
![Aktif abone](figures/ogr1_aktif_abone.png)

## 4. Periyodik yapı

- Hafta içi pik zamanı **{pr['weekday_peak_time']}** ({pr['weekday_peak_kwh']} kWh/yarım saat), en düşük yük {pr['weekday_min_time']}.
  Pik/dip oranı **{pr['peak_to_trough_ratio']}×**.
- Hafta sonu günlük tüketimi hafta içinin {pr['weekend_over_weekday_daily']} katı. Sabah piki hafta içi {pr['morning_peak_weekday']},
  hafta sonu {pr['morning_peak_weekend']} ({pr['morning_peak_shift_h']:+.1f} saat kayma).
- Hane başı ortalama günlük tüketim {pr['daily_kwh_mean']} kWh. Kış (Ara–Şub) / yaz (Haz–Ağu) oranı: **{pr['winter_summer_ratio']}**.
- {yorum_acf()}

![Gün içi profil](figures/ogr1_gun_ici_profil.png)
![Isı haritası](figures/ogr1_isi_haritasi.png)
![Mevsimsellik](figures/ogr1_mevsimsellik.png)
![ACF](figures/ogr1_acf.png)

**Sonuç:** Seri üç iç içe mevsimsellik taşır: günlük (48 dilim), haftalık (336 dilim) ve yıllık.
Saatlik/günlük/haftalık aggregation'lar (Öğrenci 3) bu periyotlara göre tasarlanmalı; tahmin modelleri
(Öğrenci 4) saat-dilimi, haftanın günü ve ay özniteliklerini mutlaka içermeli.

## 5. Değer dağılımı

| Ortalama | Std | Çarpıklık | Medyan | %99 | %99,9 | Maks | Sıfır payı |
|---|---|---|---|---|---|---|---|
| {dist['mean']:.4f} | {dist['std']:.4f} | {dist['skew']:.2f} | {dist['p50']} | {dist['p99']} | {dist['p999']} | {dist['max']} | %{100 * dist['zero_share']:.3f} |

Dağılım **sağa çarpık** (çarpıklık {dist['skew']:.1f}): okumaların çoğu düşük taban yükte, az sayıda okuma çok yüksek.
Ortalama medyandan büyük ({dist['mean']:.3f} > {dist['p50']}). Bu nedenle uç değer tespiti (Öğrenci 2) için
ortalama±3σ yerine kuantil/IQR veya log dönüşümü tabanlı yöntemler daha uygun; regresyon modellerinde
log1p(kWh) hedefi denenmeli.

![Dağılım](figures/ogr1_dagilim.png)

## 6. Demografik gruplar

""" + "\n".join(
        f"- **{col}**: " + ", ".join(f"{k} — {v['meters']} abone, ort. {v['mean_kwh_hh']} kWh/yarım saat"
                                      for k, v in m["groups"][col].items())
        for col in ("acorn_grouped", "tariff")) + f"""

{yorum_grup()}

![Grup profilleri](figures/ogr1_grup_profilleri.png)

## 7. Performans notu

| Adım | Süre (sn) |
|---|---|
""" + "\n".join(f"| {k} | {v} |" for k, v in m["timings"].items()) + "\n"
    prep = METRICS_DIR / "s02_prepare.json"
    if prep.exists() and "synthetic" in json.loads(prep.read_text(encoding="utf-8")).get("input", ""):
        md = md.replace("\n\n## 1.", "\n\n> ⚠️ **SENTETİK VERİ** — Bu rapor `s00_make_synthetic_raw` çıktısıyla üretildi. "
                        "Sayılar yalnızca kodu sınamak içindir, bulgu olarak sunulamaz.\n\n## 1.", 1)
    out = DOCS_DIR / "ogrenci1_bulgular.md"
    out.write_text(md, encoding="utf-8")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subset", default=None, help="100k | 1m | 5m (boş = tüm canonical veri)")
    args = ap.parse_args()
    ensure_dirs()
    plot_style.apply()
    spark = get_spark("ogrenci1-ts-structure")
    path = subset_path(args.subset, "parquet") if args.subset else CANONICAL_PATH
    label = f"alt küme {args.subset}" if args.subset else "canonical veri (tamamı)"
    df = spark.read.parquet(str(path)).cache()

    m, t = {"input": str(path)}, {}
    steps = [("overview", overview), ("intervals", interval_analysis), ("coverage", coverage),
             ("active", active_meters), ("profiles", profiles), ("acf", autocorrelation),
             ("distribution", value_distribution), ("groups", group_profiles)]
    for key, fn in steps:
        with timer(t, key):
            m[key] = fn(df)
        print(f"[ok] {key:<13} {t[key]:>7.2f} sn")
    m["timings"] = t
    save_metrics(m, METRICS_DIR / "s04_ts_structure.json")
    print("Rapor:", write_report(m, label))
    spark.stop()


if __name__ == "__main__":
    main()
