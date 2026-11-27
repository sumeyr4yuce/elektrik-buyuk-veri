"""Ortak veri sözleşmesi (data contract).

Ham LCL dosyası ile ekibin geri kalanının kullandığı "canonical" tablo
arasındaki eşleme burada tanımlıdır. Öğrenci 2-5 yalnızca CANONICAL kolonlara
güvenmelidir; ham kolon adları değişirse sadece bu dosya güncellenir.
"""
from pyspark.sql import types as T

# --- Ham LCL kolonları (dosyada göründüğü gibi; kWh kolonunun sonunda boşluk var) ---
RAW_COLUMNS = {
    "LCLid": "meter_id",
    "stdorToU": "tariff",
    "DateTime": "ts_raw",
    "KWH/hh (per half hour) ": "kwh_raw",
    "Acorn": "acorn",
    "Acorn_grouped": "acorn_grouped",
}

# Ham dosyayı şema çıkarımı (inferSchema) yapmadan okumak için:
# 167M satırda inferSchema veriyi iki kez taradığı için pahalıdır.
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in RAW_COLUMNS])

# --- Canonical şema (data/processed/readings.parquet) ---
CANONICAL_SCHEMA = T.StructType([
    T.StructField("meter_id", T.StringType(), False),       # MAC000002
    T.StructField("tariff", T.StringType(), True),          # Std | ToU
    T.StructField("ts", T.TimestampType(), False),          # okuma zamanı (GMT, yaz saati düzeltmesi yok)
    T.StructField("kwh", T.DoubleType(), True),             # yarım saatlik tüketim; NULL = ölçüm yok
    T.StructField("acorn", T.StringType(), True),           # ACORN-A ... ACORN-Q
    T.StructField("acorn_grouped", T.StringType(), True),   # Affluent | Comfortable | Adversity | ...
    T.StructField("date", T.DateType(), False),
    T.StructField("year", T.IntegerType(), False),
    T.StructField("month", T.IntegerType(), False),
    T.StructField("dow", T.IntegerType(), False),            # 1=Pazartesi ... 7=Pazar (ISO)
    T.StructField("hour", T.IntegerType(), False),
    T.StructField("slot", T.IntegerType(), False),           # gün içi yarım saat dilimi 0..47
    T.StructField("is_weekend", T.BooleanType(), False),
])

CANONICAL_COLUMNS = [f.name for f in CANONICAL_SCHEMA.fields]
