"""Adım 2b — ACORN demografik bilgisini hane tablosundan ekler.

Neden gerekli?
  London Datastore'un güncel LCL sürümünde (LCL-June2015v2_*.csv) yalnızca
  4 kolon var: LCLid, stdorToU, DateTime, KWH/hh. 2015'teki ilk sürümde bulunan
  Acorn / Acorn_grouped kolonları çıkarılmış. Bu bilgi, aynı verinin Kaggle
  kopyasındaki hane tablosunda (informations_households.csv) duruyor:
  https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london
  Kolonlar: LCLid, stdorToU, Acorn, Acorn_grouped, file

Bu script ham veriyi yeniden işlemeden, mevcut canonical Parquet'e
meter_id üzerinden LEFT JOIN yapar ve readings.parquet + meters.parquet'i
yeniden yazar. Hane tablosunda bulunmayan abonelerin ACORN değeri NULL kalır.

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s02b_enrich_households
    python -m src.ogrenci1_veri_hazirlama.s02b_enrich_households --households yol/informations_households.csv
"""
import argparse
import shutil

from pyspark.sql import functions as F

from src.common.config import CANONICAL_PATH, METERS_PATH, METRICS_DIR, RAW_DIR, ensure_dirs
from src.common.schema import CANONICAL_COLUMNS
from src.common.spark_session import get_spark
from src.common.utils import save_metrics, timer
from src.ogrenci1_veri_hazirlama.s02_prepare import build_meters


def read_households(spark, path):
    h = spark.read.option("header", True).csv(str(path))
    cols = {c.strip().lower(): c for c in h.columns}
    missing = {"lclid", "acorn", "acorn_grouped"} - set(cols)
    if missing:
        raise SystemExit(f"Hane dosyasında beklenen kolonlar yok: {missing}. Bulunanlar: {h.columns}")
    return (h.select(F.trim(F.col(cols["lclid"])).alias("meter_id"),
                     F.trim(F.col(cols["acorn"])).alias("acorn_new"),
                     F.trim(F.col(cols["acorn_grouped"])).alias("acorn_grouped_new"))
             .dropDuplicates(["meter_id"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--households", default=str(RAW_DIR / "informations_households.csv"))
    args = ap.parse_args()
    ensure_dirs()
    spark = get_spark("ogrenci1-enrich")
    m = {"households_file": args.households}

    with timer(m, "t_enrich_s"):
        h = read_households(spark, args.households)
        r = spark.read.parquet(str(CANONICAL_PATH))
        out = (r.join(F.broadcast(h), "meter_id", "left")
                .withColumn("acorn", F.coalesce("acorn", "acorn_new"))
                .withColumn("acorn_grouped", F.coalesce("acorn_grouped", "acorn_grouped_new"))
                .select(*CANONICAL_COLUMNS))
        tmp = CANONICAL_PATH.with_name("readings_tmp.parquet")
        (out.repartition("year", "month").sortWithinPartitions("meter_id", "ts")
            .write.mode("overwrite").partitionBy("year", "month").parquet(str(tmp)))
        shutil.rmtree(CANONICAL_PATH)          # okunan yolun üzerine doğrudan yazılamaz
        tmp.rename(CANONICAL_PATH)

        readings = spark.read.parquet(str(CANONICAL_PATH))
        build_meters(readings).coalesce(1).write.mode("overwrite").parquet(str(METERS_PATH))

    meters = spark.read.parquet(str(METERS_PATH))
    m["meters"] = meters.count()
    m["meters_with_acorn"] = meters.filter(F.col("acorn").isNotNull()).count()
    m["rows"] = readings.count()
    m["acorn_grouped_counts"] = {r[0]: r[1] for r in
                                 meters.groupBy("acorn_grouped").count().collect()}
    save_metrics(m, METRICS_DIR / "s02b_enrich.json")

    ok = m["meters_with_acorn"] == m["meters"]
    print(f"{'✅' if ok else '⚠️'} ACORN bilgisi eklenen abone: {m['meters_with_acorn']} / {m['meters']}")
    print(f"   Satır sayısı değişmedi mi? {m['rows']:,}")
    print(f"   Gruplar: {m['acorn_grouped_counts']}")
    spark.stop()


if __name__ == "__main__":
    main()
