import os
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TABLE_NAME = "taxi_data"

def store_to_dwh():
    input_dir = "/opt/airflow/data/processed/yellow_taxi"
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    if not files:
        logging.warning("Aucun fichier Parquet trouvé à charger dans DWH.")
        return

    logging.info(f"Fichiers trouvés: {files}")

    try:
        with psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        ) as conn:
            with conn.cursor() as cursor:
                for file in files:
                    logging.info(f"Chargement du fichier : {file}")
                    df = pd.read_parquet(file)
                    logging.info(f"Fichier chargé ({len(df)} lignes).")

                    if df.empty:
                        logging.warning(f"Fichier vide ignoré : {file}")
                        continue

                    records = [
                        (
                            row.get('VendorID'), 
                            row.get('tpep_pickup_datetime'), 
                            row.get('tpep_dropoff_datetime'),
                            row.get('passenger_count'), 
                            row.get('trip_distance'),
                            row.get('fare_amount'),
                            row.get('tip_amount'),
                            row.get('total_amount'),
                            row.get('pickup_date')
                        ) for _, row in df.iterrows()
                    ]

                    logging.info(f"Insertion en cours dans PostgreSQL : {len(records)} lignes.")
                    psycopg2.extras.execute_batch(cursor, f"""
                        INSERT INTO {TABLE_NAME} (
                            VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                            passenger_count, trip_distance, fare_amount, tip_amount, total_amount, pickup_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, records)
                    logging.info("Insertion terminée.")
                conn.commit()
                logging.info("✅ Données insérées avec succès dans PostgreSQL.")
    except Exception as e:
        logging.error(f"Erreur lors de l’insertion dans le DWH : {e}")

if __name__ == "__main__":
    store_to_dwh()
