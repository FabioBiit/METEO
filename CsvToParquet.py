import pandas as pd

from pathlib import Path
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

# Definisci il percorso della directory
folder_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}")

# Verifica se la cartella esiste
if not folder_path.exists():
    folder_path.mkdir(parents=True, exist_ok=True)  # Crea la directory se non esiste
    print(f"Cartella creata: {folder_path}")
else:
    print(f"La cartella esiste già: {folder_path}")

root_dir = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/")  # Percorso della cartella principale

dataframe = {} # Diz per lo storage

# Itera attraverso tutte le sotto-cartelle e i file
for i, file_path in enumerate(root_dir.rglob("*")):  # * indica tutti i file e le cartelle
    if file_path.is_file() and file_path.suffix == '.csv':  # Verifica che sia un file CSV
        print(f"Caricando il file: {file_path}")
        
        # Leggi il file CSV in un DataFrame
        df_pd = pd.read_csv(file_path)
        
        # Definisci lo schema dei dati (tipi di dato per ogni colonna)
        schema_dati = {
            'City': str,
            'Temperature_C°': float,
            'Temperature_Max_C°': float,
            'Weather_description': str,
            'Humidity_%': int,
            'Wind_speed_m/s': float,
            'Time': str  # timestamp come stringa per semplicità
        }
        
        # Applica il tipo di dato solo alle colonne esistenti nel DataFrame
        for column, dtype in schema_dati.items():
            if column in df_pd.columns:
                df_pd[column] = df_pd[column].astype(dtype, errors='ignore')  # Ignora errori se la colonna non esiste

        # Aggiungi il DataFrame al dizionario con una chiave unica
        dataframe[f"df_{i+1}"] = df_pd

df_final = pd.concat(dataframe, ignore_index=True).drop_duplicates() # Unione dei dataframe creati in precedenza in un unico DF

# Salva il DataFrame aggiornato
df_final.to_parquet(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}/StoricoMeteo_{anno}_{mese}.parquet", engine="pyarrow", compression="snappy")