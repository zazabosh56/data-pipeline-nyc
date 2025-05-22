from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql.functions import from_unixtime, col
import sys

spark = SparkSession.builder \
    .appName("WeatherBatchProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("dt", LongType(), True),
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("pressure", FloatType(), True),
    ]), True),
    StructField("name", StringType(), True)
])

print("Lecture des fichiers météo (batch)")
df = spark.read.schema(schema).option("multiline", "true").json("/opt/airflow/data/raw/weather/*.json")

print("Nombre de lignes météo lues :", df.count())
df.show(5, truncate=False)

processed = df.select(
    from_unixtime(col("dt")).cast("timestamp").alias("timestamp"),
    col("main.temp").alias("temperature"),
    col("main.humidity").alias("humidity"),
    col("main.pressure").alias("pressure"),
    col("name").alias("city")
)

print("Nombre de lignes prêtes à insérer :", processed.count())
processed.show(5, truncate=False)

print("======= APERCU AVANT WRITE =======")
processed.show(10, truncate=False)
processed.printSchema()
print("==================================")


processed.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/airflow") \
  .option("dbtable", "weather_data") \
  .option("user", "airflow") \
  .option("password", "airflow") \
  .option("driver", "org.postgresql.Driver") \
  .mode("append") \
  .save()

print("✅ Données insérées dans PostgreSQL.")

spark.stop()
