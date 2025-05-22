from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, dayofweek, when

spark = SparkSession.builder.appName("WeatherTransform").getOrCreate()

df = spark.read.option("multiline", "true").json("/opt/airflow/data/raw/weather/*.json")

df = df.withColumn("timestamp", from_unixtime(col("dt")).cast("timestamp"))
df = df.withColumn("temperature", col("main.temp"))
df = df.withColumn("humidity", col("main.humidity"))
df = df.withColumn("wind_speed", col("wind.speed"))
df = df.withColumn("weather_condition", col("weather")[0]["main"])  # <-- ici le bon nom
df = df.withColumn("city", col("name"))

# Catégorie météo
df = df.withColumn("weather_category", 
    when(col("weather_condition") == "Clear", "Clair")
    .when(col("weather_condition").isin("Rain", "Drizzle", "Thunderstorm"), "Pluvieux/Orageux")
    .otherwise("Autre")
)

df = df.withColumn("hour", hour("timestamp"))
df = df.withColumn("day_of_week", dayofweek("timestamp"))  # <-- ici le bon nom

final_df = df.select(
    "timestamp", "temperature", "humidity", "wind_speed",
    "weather_condition", "weather_category", "hour", "day_of_week", "city"
)

final_df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres:5432/airflow")\
    .option("dbtable", "dim_weather")\
    .option("user", "airflow")\
    .option("password", "airflow")\
    .option("driver", "org.postgresql.Driver")\
    .mode("append")\
    .save()

print("✅ Weather transform done → dim_weather")
spark.stop()
