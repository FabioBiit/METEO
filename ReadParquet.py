import pandas as pd

from pathlib import Path
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

df = pd.read_parquet(f'C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_PARQUET/{anno}/{mese}/StoricoMeteo_{anno}_{mese}.parquet', engine='pyarrow')

df['Time'] = pd.to_datetime(df['Time'])

#df.info()

print(df)