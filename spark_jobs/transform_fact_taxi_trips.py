from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, hour, dayofweek, round as spark_round

# 1. Lecture
spark = SparkSession.builder.appName("TaxiTransform").getOrCreate()
df = spark.read.parquet("/opt/airflow/data/raw/taxi/yellow_tripdata_2023-01.parquet")

# 2. Nettoyage/transformation
df = df.withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
df = df.withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

df = df.withColumn("duration_min", 
    (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
)

df = df.withColumn("distance_bucket", when(col("trip_distance") <= 2, "0-2 km")
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
from pyspark.sql.functions import create_map, lit
mapping_expr = create_map([lit(x) for x in sum(payment_mapping.items(), ())])
df = df.withColumn("payment_type_name", mapping_expr[col("payment_type")])

df = df.withColumn("tip_pct", spark_round((col("tip_amount") / col("fare_amount")) * 100, 2))
df = df.withColumn("pickup_hour", hour("pickup_datetime"))
df = df.withColumn("pickup_weekday", dayofweek("pickup_datetime"))

# 3. Sauvegarde dans PostgreSQL
df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/airflow")\
    .option("dbtable", "fact_taxi_trips")\
    .option("user", "airflow")\
    .option("password", "airflow")\
    .option("driver", "org.postgresql.Driver")\
    .mode("overwrite")\
    .save()

print("✅ Transformation taxi terminée, sauvegardée dans fact_taxi_trips")
spark.stop()
