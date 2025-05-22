import os
import sys
import pandas as pd
import logging
import requests

TAXI_PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LAKE_DIR = "/opt/airflow/data/raw/taxi"  # Chemin ABSOLU dans le container

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

REQUIRED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount",
    "tip_amount", "total_amount"
]

def validate_schema(df):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logging.error(f"Colonnes manquantes : {missing_cols}")
        sys.exit(1)

def download_and_save_parquet(url, dest_dir):
    filename = url.split("/")[-1]
    dest_path = os.path.join(dest_dir, filename)
    os.makedirs(dest_dir, exist_ok=True)
    if os.path.exists(dest_path):
        logging.info(f"Fichier déjà téléchargé : {dest_path}")
        return dest_path

    logging.info(f"Téléchargement : {url}")
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        logging.error(f"Échec du téléchargement ({r.status_code})")
        sys.exit(1)
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info(f"Téléchargé : {dest_path}")
    return dest_path

def process_file(file_path):
    logging.info(f"Lecture du fichier {file_path}")
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        logging.error(f"Erreur lecture parquet : {e}")
        sys.exit(1)

    if df.empty:
        logging.warning("Dataframe vide !")
        sys.exit(1)

    validate_schema(df)
    logging.info(f"Aperçu des données :\n{df.head()}")

if __name__ == "__main__":
    parquet_path = download_and_save_parquet(TAXI_PARQUET_URL, LAKE_DIR)
    process_file(parquet_path)
