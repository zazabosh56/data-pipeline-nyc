import os
import glob
import psycopg2
import pandas as pd
import logging
import tempfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TABLE_NAME = "taxi_data"

def store_to_dwh_copy():
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
                    df = df.drop_duplicates(subset=["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
                    logging.info(f"Fichier chargé ({len(df)} lignes).")

                    if df.empty:
                        logging.warning(f"Fichier vide ignoré : {file}")
                        continue

                    if "passenger_count" in df.columns:
                         df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)

                    # Écrire dans un CSV temporaire sans index, UTF-8, header adapté
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmpfile:
                        csv_path = tmpfile.name
                        df.to_csv(tmpfile, index=False, header=False)

                    logging.info(f"Insertion en masse via COPY depuis {csv_path}")

                    with open(csv_path, "r", encoding="utf-8") as f:
                        cursor.copy_expert(f"""
                            COPY {TABLE_NAME} (
                                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                                passenger_count, trip_distance, fare_amount, tip_amount, total_amount, pickup_date
                            )
                            FROM STDIN WITH CSV
                        """, f)
                    os.remove(csv_path)  # Nettoyage du fichier temporaire

                    logging.info("Insertion terminée.")

                conn.commit()
                logging.info("✅ Données insérées avec succès dans PostgreSQL.")
    except Exception as e:
        logging.error(f"Erreur lors de l’insertion dans le DWH : {e}")

if __name__ == "__main__":
    store_to_dwh_copy()
