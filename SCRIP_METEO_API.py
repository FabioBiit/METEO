import requests
import pandas as pd

from pathlib import Path
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

# Funzione per modificare solo le città che contengono "Provincia" o "Province"
def clean_city_name(city):
    if pd.notna(city) and ('Provincia' in city or 'Province' in city):
        parts = city.split(' ')
        if len(parts) > 2:
            return parts[2]
    return city

# Path
folder_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}")

# Verifico se la cartella esiste
if not folder_path.exists():
    folder_path.mkdir(parents=True, exist_ok=True)  # Crea la directory se non esiste
    print(f"Cartella creata: {folder_path}")
else:
    print(f"La cartella esiste già: {folder_path}")

citta = [
    "palermo", 
    "catania", 
    "cagliari",
    "sassari",
    "bari",
    "lecce",
    "napoli",
    "provincia di roma", 
    "milano",
    "bologna",
    "firenze",
    "genova", 
    "torino"
    ]

dataframe = {} # Dizionario per i dataframe

for i, city in enumerate(citta):

    citta_str = "https://api.openweathermap.org/data/2.5/weather?q="+city+"&appid=d772ae99953d3504ba931841f8bd77da&units=metric"

    response = requests.get(citta_str)

    # Verificare che la richiesta sia andata a buon fine
    if response.status_code == 200 and response.json()['sys']['country'] == 'IT':
        # Ottenere i dati in formato JSON
        data = response.json()

        # Estraggo i dati ritenuti rilevanti
        dati = {
            'City' :  data['name'],
            'Temperature_C' : data['main']['temp'],
            'Temperature_Min_C' : data['main']['temp_min'],
            'Temperature_Max_C' : data['main']['temp_max'],
            'Weather_description' : data['weather'][0]['description'],
            'Humidity_%' : data['main']['humidity'],
            'Wind_speed_m/s' : data['wind']['speed'],
            'Time' : data_time_stamp
        }
    
        df_pd = pd.DataFrame([dati])
        dataframe[f'df_{i+1}'] = df_pd

    else:
        print("Failed to retrieve data from the API. Status code:", response.status_code)

df_final = pd.concat(dataframe, ignore_index=True).drop_duplicates() # Unione dei dataframe creati in precedenza in un unico DF

# Applica la funzione alla colonna 'City'
df_final['City'] = df_final['City'].apply(clean_city_name)

# print(df_final)

file_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv")

if not file_path.is_file() and file_path.suffix == '.csv': 
    df_final.to_csv(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv", mode='a', header=True, index=False)
else:
    df_final.to_csv(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv", mode='a', header=False, index=False)