"""Adım 3 — Deney kümeleri: 100K / 1M / 5M kayıt, CSV ve Parquet.

Örnekleme tasarımı
------------------
* Örnekleme birimi ABONEDİR (sayaç). Bir abonenin zaman serisi bölünmez;
  rastgele satır örneklemesi (df.sample) seriyi delik deşik eder ve
  zaman serisi / pencere analizlerini bozar.
* Aboneler sabit tohumlu bir hash ile karıştırılır ve sırayla alınır, böylece
  kümeler İÇ İÇEDİR: 100K ⊂ 1M ⊂ 5M. Boyutlar arasındaki performans farkı
  "farklı aboneler"den değil yalnızca veri hacminden kaynaklanır.
* Hedef satır sayısı tam tutturulur: son abonenin serisi zaman sırasıyla
  kesilir. Kümülatif toplam bir Spark WINDOW fonksiyonu ile hesaplanır.

Sentetik ölçekleme (--upscale K)
--------------------------------
Gerçek veri hedef boyuttan küçükse (ör. Colab'da sadece birkaç dosya indirildi
veya 500M satırlık sunucu deneyi), her abone K kez klonlanır: yeni sayaç kimliği,
zaman ekseninde rastgele gün kaydırma ve %±10 tüketim ölçeği. Böylece profil
yapısı korunur ama birebir kopya oluşmaz.

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s03_make_subsets
    python -m src.ogrenci1_veri_hazirlama.s03_make_subsets --sizes 100k 1m 5m --upscale 3
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.config import (CANONICAL_PATH, METRICS_DIR, SEED, SUBSET_SIZES,
                               subset_path, ensure_dirs)
from src.common.spark_session import get_spark
from src.common.utils import dir_size_bytes, human_bytes, save_metrics, timer


def upscale(df, factor: int, seed: int = SEED):
    """Her aboneyi `factor` kez klonla (kopya 0 = orijinal)."""
    if factor <= 1:
        return df
    k = df.sparkSession.range(factor).withColumnRenamed("id", "clone")
    out = df.crossJoin(F.broadcast(k))
    h = F.abs(F.xxhash64("meter_id", "clone", F.lit(seed)))
    shift_days = F.when(F.col("clone") == 0, 0).otherwise(h % 15 - 7)      # ±7 gün
    scale = F.when(F.col("clone") == 0, 1.0).otherwise(0.9 + (h % 2001) / 10000.0)  # 0.9..1.1
    out = (out.withColumn("ts", F.col("ts") + F.make_interval(days=shift_days))
              .withColumn("kwh", F.round(F.col("kwh") * scale, 3))
              .withColumn("meter_id", F.when(F.col("clone") == 0, F.col("meter_id"))
                          .otherwise(F.concat_ws("_c", "meter_id", F.col("clone").cast("string")))))
    # zaman kaydı nedeniyle türetilmiş kolonları yeniden hesapla
    out = (out.withColumn("date", F.to_date("ts"))
              .withColumn("year", F.year("ts")).withColumn("month", F.month("ts"))
              .withColumn("dow", ((F.dayofweek("ts") + 5) % 7) + 1)
              .withColumn("hour", F.hour("ts"))
              .withColumn("slot", F.hour("ts") * 2 + (F.minute("ts") >= 30).cast("int"))
              .withColumn("is_weekend", F.col("dow") >= 6))
    return out.drop("clone")


def household_order(df, seed: int = SEED):
    """Abone başına satır sayısı + hash sırası + kümülatif satır (window)."""
    per = df.groupBy("meter_id").agg(F.count("*").alias("n"))
    per = per.withColumn("rk", F.xxhash64("meter_id", F.lit(seed)))
    w = Window.orderBy("rk", "meter_id").rowsBetween(Window.unboundedPreceding, 0)
    return per.withColumn("cum", F.sum("n").over(w)).withColumn("cum_before", F.col("cum") - F.col("n"))


def take_n(df, order, n: int):
    sel = order.filter(F.col("cum_before") < n).select("meter_id", "cum_before")
    sub = df.join(F.broadcast(sel), "meter_id")
    # sınırdaki abonenin serisini zaman sırasına göre kes
    w = Window.partitionBy("meter_id").orderBy("ts")
    sub = (sub.withColumn("i", F.row_number().over(w))
              .filter(F.col("cum_before") + F.col("i") <= n)
              .drop("i", "cum_before"))
    return sub


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=list(SUBSET_SIZES))
    ap.add_argument("--upscale", type=int, default=1)
    ap.add_argument("--partitions", type=int, default=8,
                    help="her kümenin dosya sayısı (formatlar arası adil karşılaştırma için sabit)")
    args = ap.parse_args()
    ensure_dirs()
    spark = get_spark("ogrenci1-subsets")

    df = upscale(spark.read.parquet(str(CANONICAL_PATH)), args.upscale).cache()
    total = df.count()
    order = household_order(df).cache()
    print(f"Kaynak: {total:,} satır, {order.count():,} abone (upscale x{args.upscale})")

    results = {"source_rows": total, "upscale": args.upscale, "subsets": {}}
    for name in args.sizes:
        n = SUBSET_SIZES[name]
        if n > total:
            print(f"[UYARI] {name}: hedef {n:,} > kaynak {total:,}. --upscale kullanın. Atlanıyor.")
            continue
        sub = take_n(df, order, n).repartition(args.partitions, "meter_id") \
                                  .sortWithinPartitions("meter_id", "ts").cache()
        r = {"rows": sub.count(), "meters": sub.select("meter_id").distinct().count()}
        for fmt in ("csv", "parquet"):
            path = subset_path(name, fmt)
            with timer(r, f"t_write_{fmt}_s"):
                w = sub.write.mode("overwrite")
                (w.option("header", True).csv(str(path)) if fmt == "csv" else w.parquet(str(path)))
            r[f"{fmt}_bytes"] = dir_size_bytes(path)
        r["size_ratio_csv_over_parquet"] = round(r["csv_bytes"] / r["parquet_bytes"], 2)
        results["subsets"][name] = r
        sub.unpersist()
        print(f"{name:>5}: {r['rows']:>10,} satır | {r['meters']:>5} abone | "
              f"CSV {human_bytes(r['csv_bytes']):>9} ({r['t_write_csv_s']} sn) | "
              f"Parquet {human_bytes(r['parquet_bytes']):>9} ({r['t_write_parquet_s']} sn) | "
              f"x{r['size_ratio_csv_over_parquet']}")

    save_metrics(results, METRICS_DIR / "s03_subsets.json")
    spark.stop()


if __name__ == "__main__":
    main()
