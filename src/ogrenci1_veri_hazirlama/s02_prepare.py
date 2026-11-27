"""Adım 2 — Ham LCL CSV'lerini PySpark ile canonical Parquet'e dönüştürür.

Yapılanlar (Öğrenci 1 sorumluluğu: YAPI ve TİP; veri kalitesi kararları Öğrenci 2'de):
  1. Ham CSV'ler sabit şemayla (inferSchema olmadan) okunur.
  2. Kolonlar canonical adlara çevrilir, metinler kırpılır (trim).
  3. "2012-10-12 00:30:00.0000000" -> TimestampType (ilk 19 karakter).
  4. kWh: "Null" / boş -> NULL, diğerleri DoubleType. NULL'lar SİLİNMEZ,
     Öğrenci 2'nin eksik veri analizine aynen aktarılır.
  5. Zaman serisi analizinde kullanılacak türetilmiş kolonlar eklenir:
     date, year, month, dow (ISO), hour, slot (0..47), is_weekend.
  6. Sonuç year/month'a göre bölümlenmiş (partitioned) Parquet olarak yazılır.
  7. Abone başına meta tablo (meters.parquet) çıkarılır.

Kullanım:
    python -m src.ogrenci1_veri_hazirlama.s02_prepare
"""
import argparse

from pyspark.sql import functions as F

from src.common.config import (CANONICAL_PATH, METERS_PATH, METRICS_DIR, RAW_DIR,
                               ensure_dirs)
from src.common.schema import CANONICAL_COLUMNS, RAW_COLUMNS, RAW_SCHEMA
from src.common.spark_session import get_spark
from src.common.utils import dir_size_bytes, human_bytes, save_metrics, timer


def read_raw(spark, path):
    return (spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(RAW_SCHEMA)
            .csv(str(path)))


def to_canonical(raw):
    df = raw
    for src, dst in RAW_COLUMNS.items():
        df = df.withColumnRenamed(src, dst)
    df = df.select(*[F.trim(F.col(c)).alias(c) for c in RAW_COLUMNS.values()])

    # Başlık satırı dosyalarda tekrar ediyorsa (168 parça) -> ele.
    df = df.filter(F.col("meter_id") != "LCLid")

    kwh = F.when(F.upper(F.col("kwh_raw")).isin("NULL", "") | F.col("kwh_raw").isNull(), None) \
           .otherwise(F.col("kwh_raw").cast("double"))
    ts = F.to_timestamp(F.substring("ts_raw", 1, 19), "yyyy-MM-dd HH:mm:ss")

    df = (df.withColumn("ts", ts)
            .withColumn("kwh", kwh)
            .filter(F.col("ts").isNotNull() & F.col("meter_id").isNotNull())
            .withColumn("date", F.to_date("ts"))
            .withColumn("year", F.year("ts"))
            .withColumn("month", F.month("ts"))
            # Spark dayofweek: 1=Pazar..7=Cumartesi -> ISO'ya çevir (1=Pzt..7=Paz)
            .withColumn("dow", ((F.dayofweek("ts") + 5) % 7) + 1)
            .withColumn("hour", F.hour("ts"))
            .withColumn("slot", F.hour("ts") * 2 + (F.minute("ts") >= 30).cast("int"))
            .withColumn("is_weekend", F.col("dow") >= 6))
    return df.select(*CANONICAL_COLUMNS)


def build_meters(df):
    """Abone meta tablosu: her sayacın demografisi, aktif aralığı, okuma sayısı."""
    return (df.groupBy("meter_id")
              .agg(F.first("tariff", ignorenulls=True).alias("tariff"),
                   F.first("acorn", ignorenulls=True).alias("acorn"),
                   F.first("acorn_grouped", ignorenulls=True).alias("acorn_grouped"),
                   F.min("ts").alias("first_ts"),
                   F.max("ts").alias("last_ts"),
                   F.count("*").alias("n_rows"),
                   F.count("kwh").alias("n_valid_kwh"))
              .withColumn("expected_slots",
                          ((F.unix_timestamp("last_ts") - F.unix_timestamp("first_ts")) / 1800 + 1)
                          .cast("long"))
              .withColumn("coverage", F.round(F.col("n_valid_kwh") / F.col("expected_slots"), 4)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(RAW_DIR / "lcl_csv"),
                    help="CSV dosyası veya klasörü")
    args = ap.parse_args()
    ensure_dirs()
    spark = get_spark("ogrenci1-prepare")
    m = {"input": args.input}

    with timer(m, "t_read_transform_write_s"):
        df = to_canonical(read_raw(spark, args.input))
        (df.repartition("year", "month")
           .sortWithinPartitions("meter_id", "ts")
           .write.mode("overwrite")
           .partitionBy("year", "month")
           .parquet(str(CANONICAL_PATH)))

    readings = spark.read.parquet(str(CANONICAL_PATH))
    # Güncel LCL sürümünde Acorn kolonları yok -> sessizce NULL bırakma, uyar.
    if readings.filter(F.col("acorn").isNotNull()).limit(1).count() == 0:
        m["acorn_missing_in_raw"] = True
        print("⚠️  UYARI: Ham dosyada Acorn / Acorn_grouped kolonları yok, bu kolonlar NULL kaldı.\n"
              "    Hane tablosunu (Kaggle 'smart-meters-in-london' -> informations_households.csv)\n"
              "    data/raw/ altına koyup şunu çalıştırın:\n"
              "    python -m src.ogrenci1_veri_hazirlama.s02b_enrich_households")
    with timer(m, "t_meters_s"):
        build_meters(readings).coalesce(1).write.mode("overwrite").parquet(str(METERS_PATH))

    m["rows"] = readings.count()
    m["meters"] = spark.read.parquet(str(METERS_PATH)).count()
    m["raw_csv_bytes"] = dir_size_bytes(args.input)
    m["parquet_bytes"] = dir_size_bytes(CANONICAL_PATH)
    m["compression_ratio"] = round(m["raw_csv_bytes"] / m["parquet_bytes"], 2)
    save_metrics(m, METRICS_DIR / "s02_prepare.json")

    print(f"Satır: {m['rows']:,} | Abone: {m['meters']:,}")
    print(f"Ham CSV {human_bytes(m['raw_csv_bytes'])} -> Parquet {human_bytes(m['parquet_bytes'])} "
          f"(x{m['compression_ratio']}) | süre {m['t_read_transform_write_s']} sn")
    readings.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
