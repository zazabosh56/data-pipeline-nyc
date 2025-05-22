import requests
import json
import os
from datetime import datetime
import logging

# ================= CONFIG ==================
API_KEY = "717b01395b838152da1f821b30f6d86e"
CITY = "New York"
OUTPUT_DIR = "data/raw/weather"
# ===========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_weather():
    url = (
        f"http://api.openweathermap.org/data/2.5/weather?"
        f"q={CITY}&appid={API_KEY}&units=metric"
    )
    logging.info("Requête météo à OpenWeatherMap…")
    resp = requests.get(url)
    if resp.status_code != 200:
        logging.error(f"Erreur API: {resp.status_code} - {resp.text}")
        raise Exception("Echec de récupération des données météo.")
    return resp.json()

def save_weather_data(data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(OUTPUT_DIR, f"weather_{timestamp}.json")
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logging.info(f"Données météo sauvegardées dans {filepath}")

if __name__ == "__main__":
    meteo = fetch_weather()
    save_weather_data(meteo)
