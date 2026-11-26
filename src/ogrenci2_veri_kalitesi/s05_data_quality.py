"""
Öğrenci 2 — Eksik ölçümler, uç değerler ve veri kalitesi analizi (PySpark).

Girdi : Öğrenci 1'in canonical verisi (data/processed/readings.parquet) veya bir alt küme
        (data/subsets/<100k|1m|5m>/parquet).
Çıktı : <out>/clean/readings_clean.parquet   temizlenmiş + bayraklı veri (Öğrenci 3-5 bunu okur)
        <out>/metrics/s05_data_quality.json  tüm sayılar ve süreler
        <out>/docs/ogrenci2_bulgular.md      otomatik rapor
        <out>/docs/figures/ogr2_*.png        grafikler

Temel ilke: Gerçek pikler Öğrenci 5'in araştırma sorusunun konusu. Bu yüzden istatistiksel uç değerler
SİLİNMEZ, yalnızca bayraklanır. Sadece fiziksel olarak imkânsız değerler (negatif, bağlantı kapasitesi üstü)
geçersiz sayılır.

Çalıştırma:
    python -m src.ogrenci2_veri_kalitesi.s05_data_quality --data data --out outputs/ogrenci2
    python -m src.ogrenci2_veri_kalitesi.s05_data_quality --data data --input-subset 1m --out outputs/ogrenci2_1m
    python -m src.ogrenci2_veri_kalitesi.s05_data_quality --data data --benchmark --out outputs/ogrenci2
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

os.environ.setdefault("TZ", "UTC")
try:
    time.tzset()
except AttributeError:  # Windows
    pass

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

SLOT_S = 1800  # 30 dakika
SLOTS_PER_DAY = 48

# --- Karar eşikleri (raporda gerekçeleriyle yazılır) ---
SHORT_GAP_MAX = 2        # <= 2 eksik dilim (<= 1 saat): doğrusal interpolasyon
MEDIUM_GAP_MAX = 48      # 3..48 dilim (<= 1 gün): abonenin (haftanın günü, dilim) medyan profili
                         # > 48 dilim: doldurulmaz, NULL bırakılır, 'long_gap' bayrağı
MAX_PHYSICAL_KWH = 11.5  # İngiltere tek faz 100 A × 230 V ≈ 23 kW → yarım saatte en fazla ≈ 11.5 kWh
IQR_K = 3.0              # log1p ölçeğinde Q3 + 3·IQR → global uç değer
ROBUST_Z = 5.0           # abone × dilim bazlı robust z (medyan/MAD) → bağlamsal uç değer
MAD_FLOOR = 0.05         # MAD alt sınırı (kWh): çok düzgün dilimlerde küçük oynamaların uç değer sayılmasını önler
ZERO_RUN_MIN = 48        # ≥ 1 gün kesintisiz 0 kWh → boş ev / sayaç arızası şüphesi
FLAT_RUN_MIN = 12        # ≥ 6 saat aynı sıfır-olmayan değer → donmuş sayaç şüphesi


def get_spark(app="ogrenci2_veri_kalitesi") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app)
        .master(f"local[{os.environ.get('EBV_SPARK_CORES', '*')}]")
        .config("spark.driver.memory", os.environ.get("EBV_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("EBV_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


class Timer:
    def __init__(self):
        self.t = {}

    def __call__(self, name):
        timer = self

        class _C:
            def __enter__(self_):
                self_.s = time.perf_counter()

            def __exit__(self_, *a):
                timer.t[name] = round(time.perf_counter() - self_.s, 3)
                print(f"  [{name}] {timer.t[name]} sn")

        return _C()


def input_path(data: Path, subset: str | None) -> Path:
    if subset:
        return data / "subsets" / subset / "parquet"
    return data / "processed" / "readings.parquet"


# ---------------------------------------------------------------------------
# 1) NULL ve ızgara dışı zaman damgaları
# ---------------------------------------------------------------------------
def step_nulls(df, M):
    on_grid = (F.unix_timestamp("ts") % SLOT_S) == 0
    d = df.withColumn("on_grid", on_grid)
    r = d.agg(
        F.count("*").alias("rows"),
        F.countDistinct("meter_id").alias("meters"),
        F.sum(F.col("kwh").isNull().cast("int")).alias("null_kwh"),
        F.sum((~F.col("on_grid")).cast("int")).alias("off_grid"),
        F.sum(((~F.col("on_grid")) & F.col("kwh").isNull()).cast("int")).alias("off_grid_and_null"),
        F.sum(((~F.col("on_grid")) & F.col("kwh").isNotNull()).cast("int")).alias("off_grid_with_value"),
        F.sum((F.col("on_grid") & F.col("kwh").isNull()).cast("int")).alias("on_grid_null"),
    ).first().asDict()
    # NULL'lar zamanda kümeleniyor mu? (aynı anda birçok sayaç → sistem/veri toplama olayı)
    events = (
        d.where(F.col("kwh").isNull())
        .groupBy(F.date_trunc("hour", "ts").alias("hour"))
        .agg(F.countDistinct("meter_id").alias("meters"), F.count("*").alias("rows"))
        .orderBy(F.desc("rows"))
        .limit(5)
        .toPandas()
    )
    r["null_events_top"] = [
        {"hour": str(x.hour), "meters": int(x.meters), "rows": int(x.rows)} for x in events.itertuples()
    ]
    null_in_top = int(events.rows.sum()) if len(events) else 0
    r["null_share_in_top5_hours"] = round(null_in_top / r["null_kwh"], 4) if r["null_kwh"] else 0.0
    M["nulls"] = r
    return d


# ---------------------------------------------------------------------------
# 2) Tekrarlanan (meter_id, ts) satırları
# ---------------------------------------------------------------------------
def step_duplicates(d, M):
    g = d.where("on_grid").groupBy("meter_id", "ts").agg(
        F.count("*").alias("n"),
        F.countDistinct("kwh").alias("n_vals"),
        F.avg("kwh").alias("kwh_mean"),
    )
    dups = g.where("n > 1")
    r = dups.agg(
        F.count("*").alias("dup_keys"),
        F.sum(F.col("n") - 1).alias("extra_rows"),
        F.sum((F.col("n_vals") > 1).cast("int")).alias("conflicting_keys"),
    ).first().asDict()
    r = {k: int(v or 0) for k, v in r.items()}
    pat = (
        dups.select(F.date_format("ts", "HH:mm").alias("hhmm"), F.dayofmonth("ts").alias("dom"))
        .groupBy("hhmm").count().orderBy(F.desc("count")).limit(3).toPandas()
    )
    r["top_times"] = {h: int(c) for h, c in zip(pat["hhmm"], pat["count"])}
    M["duplicates"] = r
    # Karar: aynı değerli kopyalardan birini tut; değerler çelişiyorsa ortalamayı al ve bayrakla.
    clean = g.select(
        "meter_id", "ts",
        F.col("kwh_mean").alias("kwh"),
        (F.col("n_vals") > 1).alias("dup_conflict"),
    )
    return clean


# ---------------------------------------------------------------------------
# 3) Fiziksel geçerlilik
# ---------------------------------------------------------------------------
def step_physical(base, M):
    invalid = (F.col("kwh") < 0) | (F.col("kwh") > MAX_PHYSICAL_KWH)
    r = base.agg(
        F.sum((F.col("kwh") < 0).cast("int")).alias("negative"),
        F.sum((F.col("kwh") > MAX_PHYSICAL_KWH).cast("int")).alias("above_capacity"),
    ).first().asDict()
    M["physical"] = {k: int(v or 0) for k, v in r.items()} | {"max_physical_kwh": MAX_PHYSICAL_KWH}
    return base.withColumn("phys_invalid", F.coalesce(invalid, F.lit(False))).withColumn(
        "kwh", F.when(F.col("phys_invalid"), None).otherwise(F.col("kwh"))
    )


# ---------------------------------------------------------------------------
# 4) Eksik dilimler: tam ızgara kur, boşlukları sınıfla, doldur
# ---------------------------------------------------------------------------
def step_gaps(spark, base, M):
    bounds = base.where(F.col("kwh").isNotNull()).groupBy("meter_id").agg(
        F.min("ts").alias("t0"), F.max("ts").alias("t1")
    )
    grid = bounds.select(
        "meter_id",
        F.explode(F.expr(f"sequence(t0, t1, interval {SLOT_S // 60} minutes)")).alias("ts"),
    )
    full = grid.join(base, ["meter_id", "ts"], "left")

    w_prev = Window.partitionBy("meter_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, 0)
    # Not: rowsBetween(0, unboundedFollowing) Spark'ta her satır için yeniden hesaplanır (O(n²)).
    # "Sonraki gözlem"i ters sıralı pencerede "önceki gözlem" olarak almak O(n)'dir.
    w_next = Window.partitionBy("meter_id").orderBy(F.desc("ts")).rowsBetween(Window.unboundedPreceding, 0)
    vt = F.when(F.col("kwh").isNotNull(), F.col("ts"))
    full = (
        full.withColumn("p_v", F.last("kwh", ignorenulls=True).over(w_prev))
        .withColumn("p_t", F.last(vt, ignorenulls=True).over(w_prev))
        .withColumn("n_v", F.last("kwh", ignorenulls=True).over(w_next))
        .withColumn("n_t", F.last(vt, ignorenulls=True).over(w_next))
        .withColumn(
            "gap_len",
            F.when(
                F.col("kwh").isNull(),
                ((F.unix_timestamp("n_t") - F.unix_timestamp("p_t")) / SLOT_S - 1).cast("int"),
            ),
        )
        .withColumn("dow", F.dayofweek("ts"))  # 1=Pazar (Spark) — yalnızca profil anahtarı; sonda Pzt=1'e çevrilir
        .withColumn("slot", (F.hour("ts") * 2 + (F.minute("ts") >= 30).cast("int")))
    ).cache()

    # Boşluk istatistikleri: her boşluk bir kez sayılsın diye boşluğun ilk dilimini al
    first_of_gap = full.where(F.col("kwh").isNull() & (F.col("p_t") == F.col("ts") - F.expr("interval 30 minutes")))
    gstats = first_of_gap.select(
        "gap_len",
        F.when(F.col("gap_len") <= SHORT_GAP_MAX, "kisa (<=1 saat)")
        .when(F.col("gap_len") <= MEDIUM_GAP_MAX, "orta (1 saat-1 gun)")
        .otherwise("uzun (>1 gun)").alias("cls"),
    )
    by_cls = gstats.groupBy("cls").agg(F.count("*").alias("gaps"), F.sum("gap_len").alias("slots")).toPandas()
    hist = gstats.groupBy("gap_len").count().orderBy("gap_len").toPandas()
    q = gstats.approxQuantile("gap_len", [0.5, 0.9, 0.99], 0.001) if len(hist) else [0, 0, 0]
    M["gaps"] = {
        "grid_slots": full.count(),
        "missing_slots": int(by_cls.slots.sum()) if len(by_cls) else 0,
        "n_gaps": int(by_cls.gaps.sum()) if len(by_cls) else 0,
        "by_class": {r.cls: {"gaps": int(r.gaps), "slots": int(r.slots)} for r in by_cls.itertuples()},
        "gap_len_quantiles_slots": {"p50": q[0], "p90": q[1], "p99": q[2]},
        "max_gap_slots": int(hist.gap_len.max()) if len(hist) else 0,
    }

    # Doldurma
    prof = base.where(F.col("kwh").isNotNull()).withColumn("dow", F.dayofweek("ts")).withColumn(
        "slot", F.hour("ts") * 2 + (F.minute("ts") >= 30).cast("int")
    ).groupBy("meter_id", "dow", "slot").agg(F.expr("percentile_approx(kwh, 0.5)").alias("prof_kwh"))
    full = full.join(prof, ["meter_id", "dow", "slot"], "left")

    frac = (F.unix_timestamp("ts") - F.unix_timestamp("p_t")) / (F.unix_timestamp("n_t") - F.unix_timestamp("p_t"))
    lin = F.col("p_v") + (F.col("n_v") - F.col("p_v")) * frac
    full = (
        full.withColumn(
            "impute_method",
            F.when(F.col("kwh").isNotNull(), F.lit(None))
            .when(F.col("gap_len") <= SHORT_GAP_MAX, F.lit("linear"))
            .when(F.col("gap_len") <= MEDIUM_GAP_MAX, F.lit("profile"))
            .otherwise(F.lit("none_long_gap")),
        )
        .withColumn(
            "kwh_clean",
            F.when(F.col("kwh").isNotNull(), F.col("kwh"))
            .when(F.col("impute_method") == "linear", F.round(lin, 3))
            .when(F.col("impute_method") == "profile", F.col("prof_kwh")),
        )
        .withColumn("is_imputed", F.col("kwh").isNull() & F.col("kwh_clean").isNotNull())
    )
    return full.drop("p_v", "p_t", "n_v", "n_t", "prof_kwh")


# ---------------------------------------------------------------------------
# 5) Uç değerler ve şüpheli diziler
# ---------------------------------------------------------------------------
def step_outliers(full, M):
    obs = full.where(F.col("kwh").isNotNull())
    # (a) Global: log1p ölçeğinde IQR (dağılım sağa çarpık, Öğrenci 1 §5)
    q1, q3 = obs.select(F.log1p("kwh").alias("l")).approxQuantile("l", [0.25, 0.75], 0.0005)
    upper_log = q3 + IQR_K * (q3 - q1)
    upper_kwh = float(round(2.718281828 ** upper_log - 1, 4))
    # Klasik ortalama ± 3σ ile kıyas (neden uygun olmadığını göstermek için)
    mu, sd = obs.agg(F.avg("kwh"), F.stddev("kwh")).first()
    sigma_upper = mu + 3 * sd

    # (b) Bağlamsal: abone × dilim robust z
    ms = obs.groupBy("meter_id", "slot").agg(F.expr("percentile_approx(kwh, 0.5)").alias("med"))
    tmp = obs.join(ms, ["meter_id", "slot"]).withColumn("ad", F.abs(F.col("kwh") - F.col("med")))
    mad = tmp.groupBy("meter_id", "slot").agg(F.expr("percentile_approx(ad, 0.5)").alias("mad"))
    full = full.join(ms, ["meter_id", "slot"], "left").join(mad, ["meter_id", "slot"], "left")
    rz = (F.col("kwh") - F.col("med")) / (1.4826 * F.greatest(F.col("mad"), F.lit(MAD_FLOOR)))

    # (c) Kesintisiz sıfır ve donmuş değer dizileri (run-length, gaps-and-islands)
    w = Window.partitionBy("meter_id").orderBy("ts")
    full = full.withColumn("chg", F.when(
        F.lag("kwh").over(w).eqNullSafe(F.col("kwh")), 0).otherwise(1))
    full = full.withColumn("run_id", F.sum("chg").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    full = full.withColumn("run_len", F.count("*").over(Window.partitionBy("meter_id", "run_id")))

    full = (
        full.withColumn("out_global", F.coalesce(F.col("kwh") > upper_kwh, F.lit(False)))
        .withColumn("out_context", F.coalesce(rz > ROBUST_Z, F.lit(False)))
        .withColumn("zero_run", F.coalesce((F.col("kwh") == 0) & (F.col("run_len") >= ZERO_RUN_MIN), F.lit(False)))
        .withColumn("flat_run", F.coalesce((F.col("kwh") > 0) & (F.col("run_len") >= FLAT_RUN_MIN), F.lit(False)))
        .drop("chg", "run_id", "run_len", "ad")
    )
    full = full.cache()
    r = full.agg(
        F.sum(F.col("kwh").isNotNull().cast("int")).alias("observed"),
        F.sum(F.col("out_global").cast("int")).alias("out_global"),
        F.sum(F.col("out_context").cast("int")).alias("out_context"),
        F.sum((F.col("out_global") & F.col("out_context")).cast("int")).alias("out_both"),
        F.sum((F.col("kwh") > sigma_upper).cast("int")).alias("out_3sigma"),
        F.sum(F.col("zero_run").cast("int")).alias("zero_run_slots"),
        F.sum(F.col("flat_run").cast("int")).alias("flat_run_slots"),
        F.sum((F.col("kwh") == 0).cast("int")).alias("zeros_total"),
    ).first().asDict()
    r = {k: int(v or 0) for k, v in r.items()}
    r.update(
        iqr_log1p={"q1": q1, "q3": q3, "k": IQR_K},
        global_upper_kwh=upper_kwh,
        mean_plus_3sigma_kwh=round(sigma_upper, 4),
        robust_z=ROBUST_Z,
    )
    # Uç değerler en çok hangi saatlerde? (pik saatlerinde ise gerçek pik olma ihtimali yüksek)
    hrs = full.where("out_context").groupBy(F.hour("ts").alias("h")).count().orderBy(F.desc("count")).limit(5).toPandas()
    r["context_outlier_top_hours"] = {int(h): int(c) for h, c in zip(hrs["h"], hrs["count"])}
    pk = full.groupBy(F.hour("ts").alias("h")).agg(F.avg("kwh").alias("k")).orderBy(F.desc("k")).first()
    r["mean_consumption_peak_hour"] = int(pk.h)
    pk_share = full.where(F.hour("ts").between(pk.h - 1, pk.h + 1)).agg(F.avg(F.col("out_context").cast("int"))).first()[0]
    r["context_rate_around_peak"] = round(float(pk_share or 0), 5)
    zm = full.where("zero_run").select("meter_id").distinct().count()
    r["meters_with_zero_run"] = zm
    M["outliers"] = r
    return full


def build_flags(full):
    flag = F.concat_ws(
        "|",
        F.when(F.col("phys_invalid"), "phys_invalid"),
        F.when(F.col("dup_conflict"), "dup_conflict"),
        F.when(F.col("impute_method") == "linear", "imputed_linear"),
        F.when(F.col("impute_method") == "profile", "imputed_profile"),
        F.when(F.col("impute_method") == "none_long_gap", "long_gap"),
        F.when(F.col("out_global"), "out_global"),
        F.when(F.col("out_context"), "out_context"),
        F.when(F.col("zero_run"), "zero_run"),
        F.when(F.col("flat_run"), "flat_run"),
    )
    return full.withColumn("quality_flag", F.when(flag == "", "ok").otherwise(flag)).withColumn(
        # Model eğitimi/pik analizi için önerilen filtre: gözlenmiş ya da güvenle doldurulmuş, şüpheli dizi değil
        "use_for_model",
        F.col("kwh_clean").isNotNull() & ~F.col("zero_run") & ~F.col("flat_run"),
    )


def per_meter_quality(full, meta_cols):
    return (
        full.groupBy("meter_id")
        .agg(
            F.count("*").alias("grid_slots"),
            F.avg(F.col("kwh").isNull().cast("int")).alias("missing_rate"),
            F.avg(F.col("out_context").cast("int")).alias("outlier_rate"),
            F.avg(F.col("zero_run").cast("int")).alias("zero_run_rate"),
            F.avg(F.col("use_for_model").cast("int")).alias("usable_rate"),
        )
        .orderBy("usable_rate")
    )


# ---------------------------------------------------------------------------
# Grafikler
# ---------------------------------------------------------------------------
def figures(full, M, fig_dir: Path):
    fig_dir.mkdir(parents=True, exist_ok=True)
    out = {}

    # 1) Boşluk uzunluğu dağılımı
    g = M["_gap_hist"]
    if len(g):
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(g.gap_len.clip(upper=100), g["count"], width=1.0, color="#4C72B0")
        ax.set_yscale("log")
        ax.axvline(SHORT_GAP_MAX + 0.5, color="green", ls="--", label="doğrusal ≤ 1 saat")
        ax.axvline(MEDIUM_GAP_MAX + 0.5, color="orange", ls="--", label="profil ≤ 1 gün")
        ax.set_xlabel("Boşluk uzunluğu (eksik yarım saat sayısı, 100+ birleştirildi)")
        ax.set_ylabel("Boşluk sayısı (log)")
        ax.set_title("Eksik ölçüm boşluklarının uzunluk dağılımı")
        ax.legend()
        fig.tight_layout(); fig.savefig(fig_dir / "ogr2_bosluk_dagilimi.png", dpi=120); plt.close(fig)
        out["bosluk"] = "ogr2_bosluk_dagilimi.png"

    # 2) Günlük eksik oranı (zaman içinde)
    daily = (
        full.groupBy(F.to_date("ts").alias("d"))
        .agg(F.avg(F.col("kwh").isNull().cast("int")).alias("miss"), F.countDistinct("meter_id").alias("m"))
        .orderBy("d").toPandas()
    )
    fig, ax = plt.subplots(figsize=(10, 3.5))
    ax.plot(daily.d, daily.miss * 100, lw=0.8, color="#C44E52")
    ax.set_ylabel("Eksik dilim (%)"); ax.set_title("Günlük eksik ölçüm oranı")
    fig.tight_layout(); fig.savefig(fig_dir / "ogr2_eksik_zaman.png", dpi=120); plt.close(fig)
    out["eksik_zaman"] = "ogr2_eksik_zaman.png"

    # 3) log1p dağılımı ve eşikler
    s = full.where(F.col("kwh").isNotNull()).sample(False, min(1.0, 300000 / max(M["outliers"]["observed"], 1)), 7)
    pdf = s.select("kwh").toPandas()
    import numpy as np
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    ax[0].hist(pdf.kwh, bins=200, color="#8172B2"); ax[0].set_yscale("log")
    ax[0].axvline(M["outliers"]["global_upper_kwh"], color="red", ls="--", label="log1p IQR sınırı")
    ax[0].axvline(M["outliers"]["mean_plus_3sigma_kwh"], color="gray", ls=":", label="ortalama+3σ")
    ax[0].set_xlabel("kWh / yarım saat"); ax[0].legend(); ax[0].set_title("Ham ölçek")
    ax[1].hist(np.log1p(pdf.kwh), bins=200, color="#55A868")
    ax[1].axvline(np.log1p(M["outliers"]["global_upper_kwh"]), color="red", ls="--")
    ax[1].set_xlabel("log1p(kWh)"); ax[1].set_title("log1p ölçeği")
    fig.suptitle("Tüketim dağılımı ve uç değer eşikleri")
    fig.tight_layout(); fig.savefig(fig_dir / "ogr2_uc_deger_esikleri.png", dpi=120); plt.close(fig)
    out["esik"] = "ogr2_uc_deger_esikleri.png"

    # 4) Bağlamsal uç değerlerin saat dağılımı
    hrs = full.groupBy(F.hour("ts").alias("h")).agg(
        F.avg(F.col("out_context").cast("int")).alias("r"), F.avg("kwh_clean").alias("k")
    ).orderBy("h").toPandas()
    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.bar(hrs.h, hrs.r * 100, color="#DD8452", label="bağlamsal uç değer oranı (%)")
    ax2 = ax.twinx(); ax2.plot(hrs.h, hrs.k, color="black", marker="o", ms=3, label="ort. kWh")
    ax.set_xlabel("Saat (UTC)"); ax.set_ylabel("Uç değer oranı (%)"); ax2.set_ylabel("Ortalama kWh")
    ax.set_title("Uç değerler günün hangi saatlerinde?")
    fig.tight_layout(); fig.savefig(fig_dir / "ogr2_uc_deger_saat.png", dpi=120); plt.close(fig)
    out["saat"] = "ogr2_uc_deger_saat.png"

    # 5) Örnek abone: doldurma öncesi/sonrası (en çok orta boşluğu olan abone)
    ex = (full.where("impute_method = 'profile'").groupBy("meter_id").count().orderBy(F.desc("count")).first())
    if ex:
        mid = ex.meter_id
        center = full.where((F.col("meter_id") == mid) & (F.col("impute_method") == "profile")).agg(F.min("ts")).first()[0]
        win = full.where(
            (F.col("meter_id") == mid)
            & (F.col("ts").between(F.lit(center) - F.expr("interval 2 days"), F.lit(center) + F.expr("interval 3 days")))
        ).select("ts", "kwh", "kwh_clean", "is_imputed", "out_context").orderBy("ts").toPandas()
        fig, ax = plt.subplots(figsize=(11, 3.8))
        ax.plot(win.ts, win.kwh_clean, color="#999", lw=0.8, label="temiz seri")
        ax.plot(win.ts, win.kwh, color="#4C72B0", lw=1.0, label="ham gözlem")
        im = win[win.is_imputed]; ax.scatter(im.ts, im.kwh_clean, s=8, color="orange", label="doldurulan", zorder=3)
        oc = win[win.out_context]; ax.scatter(oc.ts, oc.kwh, s=18, color="red", marker="x", label="bağlamsal uç değer", zorder=4)
        ax.set_title(f"Örnek abone {mid}: eksik doldurma ve uç değer bayrakları"); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(fig_dir / "ogr2_ornek_abone.png", dpi=120); plt.close(fig)
        out["ornek"] = "ogr2_ornek_abone.png"; M["example_meter"] = mid

    # 6) Abone kalite skoru
    pq = M["_per_meter"]
    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.hist(pq.usable_rate * 100, bins=40, color="#64B5CD")
    ax.set_xlabel("Modelde kullanılabilir dilim oranı (%)"); ax.set_ylabel("Abone sayısı")
    ax.set_title("Abone bazlı veri kalitesi")
    fig.tight_layout(); fig.savefig(fig_dir / "ogr2_abone_kalite.png", dpi=120); plt.close(fig)
    out["abone"] = "ogr2_abone_kalite.png"
    return out


# ---------------------------------------------------------------------------
# Rapor
# ---------------------------------------------------------------------------
def pct(a, b):
    return f"%{100 * a / b:.3f}" if b else "-"


def write_report(M, figs, path: Path, src: str):
    n, d, g, o, p = M["nulls"], M["duplicates"], M["gaps"], M["outliers"], M["physical"]
    gc = g["by_class"]
    cls = lambda k: gc.get(k, {"gaps": 0, "slots": 0})  # noqa: E731
    ev = "\n".join(f"| {e['hour']} | {e['meters']} | {e['rows']} |" for e in n["null_events_top"]) or "| - | - | - |"
    hrs = ", ".join(f"{h}:00 ({c})" for h, c in o["context_outlier_top_hours"].items())
    pm = M["_per_meter"]
    worst = pm.head(5)
    worst_tbl = "\n".join(
        f"| {r.meter_id} | %{100*r.missing_rate:.2f} | %{100*r.outlier_rate:.2f} | %{100*r.zero_run_rate:.2f} | %{100*r.usable_rate:.2f} |"
        for r in worst.itertuples()
    )
    bad = pm[pm.usable_rate < 0.5]
    bad_txt = (f"Verisinin yarısından azı kullanılabilir olan **{len(bad)} abone** var: {', '.join(bad.meter_id)}. "
               "Bu abone(ler) neredeyse tamamen sıfır okuma içeriyor (büyük olasılıkla boş ev veya çalışmayan sayaç). "
               "**Öneri:** Öğrenci 3-5 bu abone(ler)i analizden tamamen çıkarsın.") if len(bad) else \
              "Verisinin yarısından azı kullanılabilir olan abone yok."
    fig = lambda k, t: f"![{t}](figures/{figs[k]})" if k in figs else ""  # noqa: E731
    T = M["timings"]
    md = f"""# Öğrenci 2 — Eksik Ölçümler, Uç Değerler ve Veri Kalitesi: Bulgular

> Otomatik üretildi: {datetime.now():%Y-%m-%d %H:%M} · Girdi: `{src}`
> Bu dosyadaki sayılar `s05_data_quality.py` tarafından doldurulur.

## Özet

| Sorun | Bulgu | Karar |
|---|---|---|
| NULL kWh | {n['null_kwh']:,} satır ({pct(n['null_kwh'], n['rows'])}); {n['off_grid_and_null']:,} tanesi ızgara dışı | Satır atılır; dilim eksik sayılıp doldurma adımına girer |
| Izgara dışı zaman damgası | {n['off_grid']:,} (değer taşıyan: {n['off_grid_with_value']:,}) | Atılır |
| Tekrarlanan (abone, zaman) | {d['dup_keys']:,} anahtar, {d['extra_rows']:,} fazla satır; çelişen değer: {d['conflicting_keys']:,} | Tek kopya tutulur (çelişki varsa ortalama + `dup_conflict`) |
| Fiziksel geçersiz | negatif {p['negative']:,}, > {MAX_PHYSICAL_KWH} kWh {p['above_capacity']:,} | NULL yapılır (`phys_invalid`) |
| Eksik dilim | {g['n_gaps']:,} boşluk, {g['missing_slots']:,} dilim ({pct(g['missing_slots'], g['grid_slots'])}) | Uzunluğa göre doldurma (§3) |
| Global uç değer | {o['out_global']:,} ({pct(o['out_global'], o['observed'])}) | Bayrak `out_global`, silinmez |
| Bağlamsal uç değer | {o['out_context']:,} ({pct(o['out_context'], o['observed'])}) | Bayrak `out_context`, silinmez |
| ≥ 1 gün kesintisiz 0 | {o['zero_run_slots']:,} dilim, {o['meters_with_zero_run']} abone | Bayrak `zero_run`, modelden çıkarılır |
| ≥ 6 saat donmuş değer | {o['flat_run_slots']:,} dilim | Bayrak `flat_run`, modelden çıkarılır |

## 1. NULL değerler ve ızgara dışı zaman damgaları

- Toplam {n['rows']:,} satırda {n['null_kwh']:,} NULL kWh var. Bunların {n['off_grid_and_null']:,} tanesi aynı zamanda
  yarım saat ızgarasına oturmayan zaman damgası taşıyor; ızgara üzerinde olup NULL olan satır: {n['on_grid_null']:,}.
- NULL'ların %{100*n['null_share_in_top5_hours']:.1f}'i en yoğun {len(n['null_events_top'])} saatlik pencerede toplanıyor. Aynı anda çok sayıda sayacı
  etkilediği için bu bir hane davranışı değil, **veri toplama sistemi kaynaklı bir olay**:

| Saat | Etkilenen abone | NULL satır |
|---|---|---|
{ev}

**Karar:** Bu satırlar bilgi taşımadığı (değer yok, zaman ızgara dışı) için atılır. Ait oldukları yarım saat dilimi zaten
ızgarada ayrı bir satırla temsil edildiğinden veri kaybı oluşmaz.

## 2. Tekrarlanan kayıtlar

- {d['dup_keys']:,} (abone, zaman) anahtarı birden fazla kez geçiyor; bunlar {d['extra_rows']:,} fazla satır üretiyor.
- Değerleri birbirinden farklı olan tekrar sayısı: **{d['conflicting_keys']}**. {"Yani tekrarların tamamı birebir kopya." if d['conflicting_keys'] == 0 else "Çelişen tekrarlarda ortalama alınıp `dup_conflict` bayrağı kondu."}
- En sık görüldükleri saat: {', '.join(f'{k} ({v:,})' for k, v in d['top_times'].items())}. Kopyaların gece yarısında
  yoğunlaşması, ham dosyalar birleştirilirken gün/ay sınırındaki satırın iki dosyada da yer aldığını düşündürüyor.

**Karar:** Kopyalardan biri tutulur. Bu adım yapılmazsa Öğrenci 3'ün `sum` aggregation'ları ilgili günleri şişirir.

## 3. Eksik ölçümler ve doldurma stratejisi

Her abone için ilk ve son okuması arasında tam 30 dakikalık ızgara kuruldu (Spark `sequence` + `explode`)
ve gözlemler bu ızgaraya `left join` ile oturtuldu. Toplam ızgara: {g['grid_slots']:,} dilim.

| Boşluk sınıfı | Boşluk | Eksik dilim | Doldurma yöntemi |
|---|---|---|---|
| Kısa (≤ {SHORT_GAP_MAX} dilim, ≤ 1 saat) | {cls('kisa (<=1 saat)')['gaps']:,} | {cls('kisa (<=1 saat)')['slots']:,} | Doğrusal interpolasyon (önceki ve sonraki gözlem) |
| Orta (≤ {MEDIUM_GAP_MAX} dilim, ≤ 1 gün) | {cls('orta (1 saat-1 gun)')['gaps']:,} | {cls('orta (1 saat-1 gun)')['slots']:,} | Abonenin aynı (haftanın günü, yarım saat) medyanı |
| Uzun (> 1 gün) | {cls('uzun (>1 gun)')['gaps']:,} | {cls('uzun (>1 gun)')['slots']:,} | Doldurulmaz, NULL + `long_gap` |

Medyan boşluk {g['gap_len_quantiles_slots']['p50']:.0f} dilim, %90'lık {g['gap_len_quantiles_slots']['p90']:.0f},
en uzun {g['max_gap_slots']:,} dilim ({g['max_gap_slots']/48:.1f} gün).

**Gerekçe:** Öğrenci 1'in ACF analizi (1 dilim gecikmede r ≈ 0.97) kısa vadede serinin çok düzgün olduğunu gösteriyor,
bu yüzden 1 saate kadar doğrusal interpolasyon güvenli. Daha uzun boşluklarda doğrusal çizgi günlük döngüyü (akşam piki)
silerdi; bunun yerine abonenin kendi gün-içi/hafta profili kullanıldı (günlük r ≈ 0.95, haftalık r ≈ 0.93).
1 günden uzun boşluklar uydurma veri üretmemek için boş bırakıldı.

**Sınırlama:** Profil (medyan) ile doldurulan dilimler gerçek seriden daha düzgündür; kısa süreli sıçramaları içermez
(örnek abone grafiğinde turuncu bölüm). Doldurulan dilim oranı çok düşük ({pct(g['missing_slots'], g['grid_slots'])})
olduğu için genel sonuçları etkilemez, ama pik analizi yapılırken `is_imputed = true` dilimler dışarıda bırakılabilir.

{fig('bosluk', 'Boşluk dağılımı')}
{fig('eksik_zaman', 'Eksik zaman')}
{fig('ornek', 'Örnek abone')}

## 4. Uç değerler

Öğrenci 1'in bulduğu gibi dağılım çok sağa çarpık. Bu yüzden iki yöntem birlikte kullanıldı:

1. **Global (log1p IQR):** log1p(kWh) üzerinde Q3 + {IQR_K}·IQR → sınır **{o['global_upper_kwh']} kWh**.
   {o['out_global']:,} okuma ({pct(o['out_global'], o['observed'])}).
   Kıyas için klasik ortalama + 3σ sınırı {o['mean_plus_3sigma_kwh']} kWh olurdu ve {o['out_3sigma']:,} okuma
   ({pct(o['out_3sigma'], o['observed'])}) yakalardı. İki sınırın da yaklaşık 1 kWh civarına düşmesi önemli bir şey söylüyor:
   tüm hanelere tek bir eşik uygulamak, yüksek tüketimli hanelerin *normal* akşam tüketimini uç değer sayar,
   düşük tüketimli bir hanenin gerçekten olağandışı sıçramasını ise kaçırır. Bu yüzden global yöntem yalnızca kaba bir
   gösterge olarak tutuldu; asıl karar bağlamsal yönteme dayanıyor.
2. **Bağlamsal (abone × yarım saat robust z):** Her abone ve dilim için medyan ve MAD hesaplandı;
   (kWh − medyan) / (1.4826·max(MAD, {MAD_FLOOR})) > {ROBUST_Z} olan okumalar bayraklandı: {o['out_context']:,}
   ({pct(o['out_context'], o['observed'])}). İkisinin kesişimi: {o['out_both']:,}.
   Bu yöntem "bu hane için bu saatte olağandışı" olanı yakalar; az tüketen bir hanenin ani sıçraması da görünür olur.

Bağlamsal uç değerlerin en sık görüldüğü saatler (UTC): {hrs}. Ortalama tüketimin piki ise saat
**{o['mean_consumption_peak_hour']}:00**; pik çevresinde (±1 saat) bağlamsal uç değer oranı %{100*o['context_rate_around_peak']:.2f}.
{"Yani uç değerler pik çevresinde genel ortalamadan (%" + f"{100*o['out_context']/max(o['observed'],1):.2f}" + ") daha sık değil. Akşam piki hanelerin her gün tekrarlanan *düzenli* davranışı olduğu için abonenin kendi medyanı zaten yüksek ve bağlamsal yöntem onu uç değer saymıyor. Gündüz saatlerinde ise medyan düşük olduğundan çamaşır makinesi, fırın, su ısıtıcı gibi kısa süreli yükler olağandışı görünüyor. Pik tahmini açısından bu iyi haber: akşam piki gürültü değil, öğrenilebilir bir örüntü." if o['context_rate_around_peak'] <= o['out_context']/max(o['observed'],1) else "Uç değerler pik çevresinde genel ortalamadan (%" + f"{100*o['out_context']/max(o['observed'],1):.2f}" + ") daha sık görülüyor. Bu okumaların bir kısmı hanenin kendi normalinin üstüne çıkan gerçek pik talep anları olabilir; silinmemeleri bu yüzden kritik ve Öğrenci 5'in pik analizinde ayrıca incelenmeli."}

**Karar: uç değerler silinmez.** Projenin araştırma sorusu pik talebi tahmin etmek. Yüksek okumaları silmek,
tam olarak tahmin etmeye çalıştığımız olayı veriden çıkarmak olur. Bunun yerine `out_global` / `out_context` bayrakları
eklendi; Öğrenci 4 ve 5 bunları öznitelik olarak veya duyarlılık analizi için kullanabilir.

{fig('esik', 'Eşikler')}
{fig('saat', 'Uç değer saatleri')}

## 5. Şüpheli diziler (sayaç arızası / boş ev)

- **Kesintisiz sıfır:** ≥ {ZERO_RUN_MIN} dilim (1 gün) boyunca 0 kWh: {o['zero_run_slots']:,} dilim, {o['meters_with_zero_run']} abone.
  Toplam sıfır okuma {o['zeros_total']:,}; bunların büyük kısmı kısa süreli ve normal (gece taban yükü çok düşük haneler).
  Bir gün boyunca hiç tüketim olmaması ise buzdolabı gibi sürekli yükler düşünüldüğünde ya boş ev ya da sayaç/iletişim arızası.
- **Donmuş değer:** ≥ {FLAT_RUN_MIN} dilim aynı sıfır-olmayan değer: {o['flat_run_slots']:,} dilim.

**Karar:** Bu dilimler `use_for_model = false` olarak işaretlendi. Boş ev dönemleri pik tahmin modelini aşağı çeker.

## 6. Abone bazlı kalite

{bad_txt}

En düşük kaliteli {len(worst)} abone:

| Abone | Eksik | Bağlamsal uç | Sıfır dizisi | Kullanılabilir |
|---|---|---|---|---|
{worst_tbl}

{fig('abone', 'Abone kalitesi')}

## 7. Ekip için çıktı: `clean/readings_clean.parquet`

Öğrenci 1'in sözleşmesindeki kolonlara ek olarak:

| Kolon | Açıklama |
|---|---|
| kwh | ham değer (tekrar/geçersiz temizlendi, eksikse NULL) |
| kwh_clean | doldurulmuş değer (uzun boşluklarda NULL) |
| is_imputed | bu dilim dolduruldu mu |
| impute_method | linear / profile / none_long_gap / NULL |
| out_global, out_context | uç değer bayrakları |
| zero_run, flat_run | şüpheli dizi bayrakları |
| quality_flag | tüm bayrakların dikey çizgiyle birleşimi (ör. `imputed_linear\|out_context`), temizse `ok` |
| use_for_model | **önerilen filtre**: `kwh_clean` dolu ve şüpheli dizi değil |

```python
df = spark.read.parquet("outputs/ogrenci2/clean/readings_clean.parquet")
train = df.where("use_for_model")          # Öğrenci 4
daily = df.groupBy("meter_id", "date").agg(F.sum("kwh_clean"))   # Öğrenci 3
```

## 8. Performans

| Adım | Süre (sn) |
|---|---|
""" + "\n".join(f"| {k} | {v} |" for k, v in T.items()) + "\n"
    if "benchmark" in M:
        md += "\n### Ölçeklenebilirlik (aynı pipeline, iç içe alt kümeler)\n\n| Alt küme | Satır | Abone | Süre (sn) | Satır/sn |\n|---|---|---|---|---|\n"
        for k, b in M["benchmark"].items():
            md += f"| {k} | {b['rows']:,} | {b['meters']} | {b['seconds']} | {b['rows_per_s']:,.0f} |\n"
    path.write_text(md, encoding="utf-8")


# ---------------------------------------------------------------------------
def run_pipeline(spark, src: Path, M: dict, T: Timer):
    df = spark.read.parquet(str(src))
    with T("1_nulls"):
        d = step_nulls(df, M)
    with T("2_duplicates"):
        base = step_duplicates(d, M)
    with T("3_physical"):
        base = step_physical(base, M).cache()
        base.count()
    with T("4_gaps_impute"):
        full = step_gaps(spark, base, M)
    with T("5_outliers"):
        full = step_outliers(full, M)
    full = build_flags(full)
    # Öğrenci 1 sözleşmesindeki takvim kolonlarını yeniden türet (ızgaraya eklenen satırlar için de dolu olsun)
    meta = spark.read.parquet(str(src)).select("meter_id", "tariff", "acorn", "acorn_grouped").dropDuplicates(["meter_id"])
    full = (
        full.join(meta, "meter_id", "left")
        .withColumn("date", F.to_date("ts"))
        .withColumn("dow", ((F.dayofweek("ts") + 5) % 7) + 1)  # 1=Pazartesi … 7=Pazar
        .withColumn("hour", F.hour("ts"))
        .withColumn("is_weekend", F.col("dow") >= 6)
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.month("ts"))
    )
    return full, base


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data", help="Öğrenci 1'in ebv_data klasörü (veya repo içindeki data/)")
    ap.add_argument("--input-subset", default=None, choices=[None, "100k", "1m", "5m"])
    ap.add_argument("--out", default="outputs/ogrenci2", help="Çıktılar buraya yazılır (Drive'daki ebv_data DEĞİL)")
    ap.add_argument("--no-write", action="store_true", help="Temiz parquet'i yazma (hızlı deneme)")
    ap.add_argument("--benchmark", action="store_true", help="100k/1m/5m alt kümelerinde süre ölç")
    a = ap.parse_args()

    data, out = Path(a.data), Path(a.out)
    if out.resolve().is_relative_to(data.resolve()):
        raise SystemExit("Çıktı klasörü veri klasörünün içinde olamaz; ortak veriyi bozmamak için --out'u değiştirin.")
    (out / "metrics").mkdir(parents=True, exist_ok=True)
    (out / "docs" / "figures").mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    src = input_path(data, a.input_subset)
    print(f"Girdi: {src}")
    M, T = {"input": str(src)}, Timer()

    full, base = run_pipeline(spark, src, M, T)

    if not a.no_write:
        with T("6_write_clean"):
            (full.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month")
             .parquet(str(out / "clean" / "readings_clean.parquet")))

    with T("7_figures"):
        M["_gap_hist"] = (
            full.where(F.col("kwh").isNull() & F.col("gap_len").isNotNull())
            .groupBy("gap_len").agg((F.count("*") / F.col("gap_len")).alias("count"))
            .orderBy("gap_len").toPandas()
        )
        M["_per_meter"] = per_meter_quality(full, None).toPandas()
        M["_per_meter"].to_csv(out / "metrics" / "s05_meter_quality.csv", index=False)
        figs = figures(full, M, out / "docs" / "figures")

    if a.benchmark:
        M["benchmark"] = {}
        for s in ["100k", "1m", "5m"]:
            p = input_path(data, s)
            if not p.exists():
                continue
            spark.catalog.clearCache()
            t0 = time.perf_counter()
            Mb, Tb = {}, Timer()
            f, _ = run_pipeline(spark, p, Mb, Tb)
            f.agg(F.sum("kwh_clean"), F.sum(F.col("out_context").cast("int"))).collect()
            sec = round(time.perf_counter() - t0, 2)
            rows = Mb["nulls"]["rows"]
            M["benchmark"][s] = {"rows": rows, "meters": Mb["nulls"]["meters"], "seconds": sec,
                                 "rows_per_s": rows / sec, "steps": Tb.t}
            print(f"  benchmark {s}: {sec} sn")

    M["timings"] = T.t
    M["thresholds"] = dict(MAD_FLOOR=MAD_FLOOR, SHORT_GAP_MAX=SHORT_GAP_MAX, MEDIUM_GAP_MAX=MEDIUM_GAP_MAX,
                           MAX_PHYSICAL_KWH=MAX_PHYSICAL_KWH, IQR_K=IQR_K, ROBUST_Z=ROBUST_Z,
                           ZERO_RUN_MIN=ZERO_RUN_MIN, FLAT_RUN_MIN=FLAT_RUN_MIN)
    write_report(M, figs, out / "docs" / "ogrenci2_bulgular.md", str(src))
    js = {k: v for k, v in M.items() if not k.startswith("_")}
    (out / "metrics" / "s05_data_quality.json").write_text(json.dumps(js, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"Bitti. Rapor: {out/'docs'/'ogrenci2_bulgular.md'}")
    spark.stop()


if __name__ == "__main__":
    main()
