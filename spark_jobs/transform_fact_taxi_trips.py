from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, hour, dayofweek, round as spark_round, create_map, lit

# Initialisation Spark
spark = SparkSession.builder \
    .appName("TaxiTransform") \
    .getOrCreate()

# 1. Extraction des données brutes
df = spark.read.parquet("/opt/airflow/data/raw/taxi/yellow_tripdata_2023-01.parquet")

# 2. Nettoyage/transformation
df = df.withColumn("pickup_time", col("tpep_pickup_datetime").cast("timestamp"))
df = df.withColumn("dropoff_time", col("tpep_dropoff_datetime").cast("timestamp"))

df = df.withColumn(
    "trip_duration",
    (unix_timestamp("dropoff_time") - unix_timestamp("pickup_time")) / 60
)

df = df.withColumn(
    "distance_bin",
    when(col("trip_distance") <= 2, "0-2 km")
    .when((col("trip_distance") > 2) & (col("trip_distance") <= 5), "2-5 km")
    .otherwise(">5 km")
)

# Table de correspondance payment
payment_mapping = {
    "1": "Credit card",
    "2": "Cash",
    "3": "No charge",
    "4": "Dispute",
    "5": "Unknown",
    "6": "Voided trip"
}
mapping_expr = create_map([lit(x) for x in sum(payment_mapping.items(), ())])
df = df.withColumn("payment_type", mapping_expr[col("payment_type")])

df = df.withColumn("tip_percent", spark_round((col("tip_amount") / col("fare_amount")) * 100, 2))
df = df.withColumn("pickup_hour", hour("pickup_time"))
df = df.withColumn("pickup_day_of_week", dayofweek("pickup_time"))

# Sélection finale selon la table cible
final_df = df.select(
    col("VendorID"),
    col("pickup_time"),
    col("dropoff_time"),
    col("trip_duration"),
    col("distance_bin"),
    col("payment_type"),
    col("tip_percent"),
    col("pickup_hour"),
    col("pickup_day_of_week"),
    col("PULocationID").alias("pickup_zone"),
    col("DOLocationID").alias("dropoff_zone")
)

# Déduplication
final_df = final_df.dropDuplicates(["VendorID", "pickup_time", "dropoff_time"])
final_df = final_df.repartition(8)  # Ecriture parallèle

# 3. Chargement dans PostgreSQL — Aucune étape manuelle
postgres_url = "jdbc:postgresql://postgres:5432/airflow"
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

final_df.write \
    .jdbc(
        url=postgres_url,
        table="fact_taxi_trips",
        mode="append",  # 'append' pour insérer (pas 'overwrite' sinon tu perds les données !)
        properties=postgres_properties
    )

print("✅ Transformation Spark terminée ET données chargées dans PostgreSQL : fact_taxi_trips.")

spark.stop()
