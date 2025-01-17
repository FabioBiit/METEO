import pandas as pd

from pathlib import Path
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month

# Funzione per modificare solo le città che contengono "Provincia" o "Province"
def clean_city_name(city):
    if pd.notna(city) and ('Provincia' in city or 'Province' in city):
        parts = city.split(' ')
        if len(parts) > 2:  # Assicurati che ci sia una terza parte disponibile
            return parts[2]
    return city

dataframe = {}

for i in range(14, 18):

    giorno = i
    # Definisci il percorso della directory
    folder_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}")

    df_final = pd.read_csv(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv")

    # Applica la funzione alla colonna 'City'
    df_final['City'] = df_final['City'].apply(clean_city_name)

    df_final_clean = df_final[df_final['City'] != 'Rome']

    # print(df_final)

    dataframe[f"df_{i+1}"] = df_final_clean

    dataframe[f"df_{i+1}"].to_csv(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/{giorno}/Meteo_{anno}_{mese}_{giorno}.csv", header=True, index=False)