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

# Leggi il file CSV
df = pd.read_csv(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/*/*.csv")

# Salva il DataFrame aggiornato
df.to_parquet(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}/StoricoMeteo_{anno}_{mese}.parquet", engine="pyarrow", compression="snappy")