"""Tek tip SparkSession oluşturucu (yerel mod / Colab / sunucu).

ÖNEMLİ — saat dilimi: LCL zaman damgaları GMT'dir ve yaz saati düzeltmesi
içermez. Makinenin yerel saat dilimi (ör. Europe/Istanbul) devreye girerse
Spark -> pandas dönüşümünde yaz saati geçiş anlarında (ör. 2012-10-28 01:00)
zaman damgaları çakışır ya da kayar. Bu yüzden Python, JVM ve Spark SQL
oturumu UTC'ye sabitlenir.
"""
import os
import time

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

# Sürücü JVM'i builder.config'den ÖNCE başlar; JVM seçenekleri bu yüzden
# PYSPARK_SUBMIT_ARGS ile verilir. Java 17/21 + Arrow (toPandas) için netty
# yansıma izni gerekir.
JAVA_OPTS = ("-Duser.timezone=UTC -Dio.netty.tryReflectionSetAccessible=true "
             "--add-opens=java.base/java.nio=ALL-UNNAMED "
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f'--driver-java-options "{JAVA_OPTS}" pyspark-shell')

from pyspark.sql import SparkSession  # noqa: E402


def get_spark(app_name: str = "elektrik-buyuk-veri", cores: str | None = None,
              driver_memory: str | None = None) -> SparkSession:
    cores = cores or os.environ.get("EBV_SPARK_CORES", "*")
    driver_memory = driver_memory or os.environ.get("EBV_DRIVER_MEMORY", "4g")
    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_memory)
        # Arrow varsayılan KAPALI: Java 21'de bazı kurulumlarda DirectByteBuffer hatası
        # veriyor; toPandas'a giden tablolar zaten küçük özetler. Açmak için EBV_ARROW=1
        .config("spark.sql.execution.arrow.pyspark.enabled", os.environ.get("EBV_ARROW", "0") == "1")
        .config("spark.sql.session.timeZone", "UTC")         # LCL zamanları GMT; dönüşüm olmasın
        .config("spark.sql.shuffle.partitions", os.environ.get("EBV_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
