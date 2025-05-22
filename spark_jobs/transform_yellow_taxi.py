from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os
import sys
import argparse
import logging

# Configuration du logging Spark (stdout)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

REQUIRED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
    "passenger_count", "trip_distance", "fare_amount", 
    "tip_amount", "total_amount"
]

def main(input_path, output_path):
    spark = SparkSession.builder \
        .appName("Transform Yellow Taxi Data") \
        .getOrCreate()

    logging.info(f"Lecture du fichier parquet depuis {input_path} ...")
    try:
        df = spark.read.parquet(input_path)
        print(">>> Nombre de lignes lues dans le fichier source :", df.count())
        df.show(5)
    except Exception as e:
        logging.error(f"Erreur lecture Parquet : {e}")
        spark.stop()
        sys.exit(1)

    # Vérification des colonnes requises
    actual_cols = set(df.columns)
    if not set(REQUIRED_COLUMNS).issubset(actual_cols):
        missing = set(REQUIRED_COLUMNS) - actual_cols
        logging.error(f"Colonnes manquantes : {missing}")
        spark.stop()
        sys.exit(1)

    logging.info("Filtrage et nettoyage des données ...")
    df_cleaned = df.filter(
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0)
    )

    logging.info("Sélection et transformation des colonnes ...")
    df_transformed = df_cleaned.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount"
    ).withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

    logging.info("Aperçu des données transformées :")
    df_transformed.show(10, truncate=False)

    os.makedirs(output_path, exist_ok=True)
    logging.info(f"Sauvegarde des données transformées dans {output_path} ...")
    df_transformed.write.mode("overwrite").parquet(output_path)

    logging.info(f"✅ Données sauvegardées dans : {output_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformation batch Yellow Taxi Spark.")
    parser.add_argument("--input_path", type=str, required=True, help="Chemin d'entrée parquet brut.")
    parser.add_argument("--output_path", type=str, required=True, help="Dossier de sortie pour les données transformées.")
    args = parser.parse_args()
    main(args.input_path, args.output_path)
