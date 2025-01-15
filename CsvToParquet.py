import pandas as pd
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

# Leggi il file CSV
df = pd.read_csv("C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/DataFrameMeteo.csv")

# Salva il DataFrame come file Parquet
df.to_parquet(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}/{giorno}/StoricoMeteo.parquet", engine="pyarrow", compression="snappy")
