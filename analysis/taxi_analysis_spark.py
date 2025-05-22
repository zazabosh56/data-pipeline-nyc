from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.functions import when

spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .config("spark.jars", "file:///C:/Users/dell/Documents/2024-2025/DATA/4DDEV/data-pipeline-nyc/jars/postgresql-42.2.18.jar") \
    .getOrCreate()

# Charge la table des trajets taxi depuis Postgres
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/airflow") \
    .option("dbtable", "fact_taxi_trips") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()



df = df.withColumn(
    "trip_duration_min",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)
df.select("trip_duration_min").describe().show()

# Option : Sauvegarde un histogramme (nécessite matplotlib et pandas)
durations = df.select("trip_duration_min").rdd.flatMap(lambda x: x).collect()

plt.hist(durations, bins=50)
plt.title("Distribution des durées de trajets (minutes)")
plt.xlabel("Durée (min)")
plt.ylabel("Nombre de trajets")
plt.savefig("trip_duration_hist.png")
plt.close()

long_trips = df.filter(col("trip_duration_min") > 30)
short_trips = df.filter(col("trip_duration_min") <= 30)

print("Pourboire moyen sur les longs trajets :")
long_trips.select("tip_amount").describe().show()

print("Pourboire moyen sur les trajets courts :")
short_trips.select("tip_amount").describe().show()


df = df.withColumn("tip_percent", when(col("total_amount") > 0, col("tip_amount") / col("total_amount")).otherwise(None))
corr = df.corr("trip_distance", "tip_percent")
print(f"Corrélation distance / pourcentage pourboire : {corr:.3f}")



