import os
import requests
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# =^.^=

# Carica la configurazione (API key) dal file .env
load_dotenv()
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')

# Configurazione del logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Funzione per ottenere i dati meteo da OpenWeatherMap
def fetch_weather_data(city, api_key):
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        
        # Verifica se la richiesta ha avuto successo
        if response.status_code == 200:
            data = response.json()
            
            # Verifica che i dati siano completi
            if 'main' in data and 'weather' in data:
                return data
            else:
                logger.error(f"Missing expected data in response for city: {city}")
                return None
        else:
            logger.error(f"Failed to retrieve data for {city}. Status code: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching data for {city}: {e}")
        return None

# Funzione per pulire il nome della città
def clean_city_name(city):
    if pd.notna(city) and ('Provincia' in city or 'Province' in city):
        parts = city.split(' ')
        if len(parts) > 2:
            return parts[2]
    return city

# Funzione per creare la directory se non esiste
def create_directory(path):
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cartella creata: {path}")
    else:
        logger.info(f"La cartella esiste già: {path}")

# Funzione principale per raccogliere i dati meteo e salvarli in un CSV
def collect_weather_data(cities, api_key):
    data_time_stamp = datetime.now().replace(microsecond=0)
    anno, mese, giorno = data_time_stamp.year, data_time_stamp.month, data_time_stamp.day

    folder_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}")
    create_directory(folder_path)
    
    dataframe = []

    for city in cities:
        data = fetch_weather_data(city, api_key)
        if data:
            weather_data = {
                'City': data['name'],
                'Temperature_C': data['main']['temp'],
                'Temperature_Min_C': data['main']['temp_min'],
                'Temperature_Max_C': data['main']['temp_max'],
                'Weather_description': data['weather'][0]['description'],
                'Humidity_%': data['main']['humidity'],
                'Wind_speed_m/s': data['wind']['speed'],
                'Time': data_time_stamp
            }
            dataframe.append(weather_data)
    
    # Creazione del dataframe finale
    df_final = pd.DataFrame(dataframe).drop_duplicates()
    
    # Pulizia dei nomi delle città
    df_final['City'] = df_final['City'].apply(clean_city_name)
    
    # Salvataggio su CSV
    file_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv")
    header = not file_path.is_file()
    df_final.to_csv(file_path, mode='a', header=header, index=False)
    logger.info(f"Data saved to {file_path}")

# Lista delle città da monitorare
cities = [
    "palermo", "catania", "cagliari", "sassari", "bari", "lecce",
    "napoli", "provincia di roma", "milano", "bologna", "firenze",
    "genova", "torino"
]

# Esegui la raccolta dei dati meteo
collect_weather_data(cities, API_KEY)