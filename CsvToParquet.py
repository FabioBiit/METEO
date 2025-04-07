import pandas as pd

from pathlib import Path
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

# Definisci il percorso della directory per il salvataggio del file parquet
folder_path = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}")

# Verifica se la cartella esiste
if not folder_path.exists():
    folder_path.mkdir(parents=True, exist_ok=True)  # Crea la directory se non esiste
    print(f"Cartella creata: {folder_path}")
else:
    print(f"La cartella esiste già: {folder_path}")

root_dir = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/")  # Percorso della cartella principale

# dataframe = {} # Diz per lo storage

dataframe = [] # Lista per lo storage

# Itera attraverso tutte le sotto-cartelle e i file
# for i, file_path in enumerate(root_dir.rglob("*")):  # * indica tutte le cartelle i file in esse contenuti

for file_path in root_dir.rglob("*"):  # * indica tutte le cartelle i file in esse contenuti
    if file_path.is_file() and file_path.suffix == '.csv':  # Verifica che sia un file CSV
        print(f"Caricando il file: {file_path}")
        
        # Leggi il file CSV in un DataFrame
        df_pd = pd.read_csv(file_path, low_memory=False)

        dataframe.append(df_pd)

        # Aggiungi il DataFrame al dizionario con una chiave unica
        #dataframe[f"df_{i+1}"] = df_pd

#df_final = pd.concat(dataframe)  # Unione dei dataframe creati in precedenza in un unico DF

df_final = pd.concat(dataframe, ignore_index=True).drop_duplicates() # Unione dei dataframe creati in precedenza in un unico DF

# print(df_final)

# Salva il DataFrame aggiornato
df_final.to_parquet(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}/StoricoMeteo_{anno}_{mese}.parquet", engine="pyarrow", compression="snappy")