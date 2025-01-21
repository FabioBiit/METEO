import pandas as pd

from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace

data_time_stamp = datetime.now().replace(microsecond=0)

anno = data_time_stamp.year
mese = data_time_stamp.month
giorno = data_time_stamp.day

# Creare una sessione Spark
spark = SparkSession.builder \
    .appName("MeteoSpark") \
    .getOrCreate()

root_dir = Path(f"C:/Users/kyros/OneDrive/Desktop/METEO/STORICO_ROW_CSV/{anno}/{mese}/")  # Percorso della cartella principale

dataframe = {} # Diz per lo storage

# Itera attraverso tutte le sotto-cartelle e i file
for i, file_path in enumerate(root_dir.rglob("*")):  # * indica tutte le cartelle i file in esse contenuti
    if file_path.is_file() and file_path.suffix == '.csv':  # Verifica che sia un file CSV
        print(f"Caricando il file: {file_path}")
        
        # Leggi il file CSV in un DataFrame
        df_pd = pd.read_csv(file_path, low_memory=False)

        # Aggiungi il DataFrame al dizionario con una chiave unica
        dataframe[f"df_{i+1}"] = df_pd

df_final = pd.concat(dataframe, ignore_index=True).drop_duplicates() # Unione dei dataframe creati in precedenza in un unico DF

# Converti il DataFrame in un DataFrame Spark
df_spark = spark.createDataFrame(df_final)

df_spark = df_spark.withColumn(
    "Temperature_C",
    when( (col("City") == 'Rome') & (col("Temperature_C") < 0),
        regexp_replace(col('Temperature_C').cast("string"), '-', '').cast("double")).otherwise(col('Temperature_C')))\
    .withColumn("Temperature_Min_C", when( (col("City") == 'Rome') & (col("Temperature_Min_C") < 0),
        regexp_replace(col('Temperature_Min_C').cast("string"), '-', '').cast("double")).otherwise(col("Temperature_Min_C")))\
    .withColumn("Temperature_Max_C", when( (col("City") == 'Rome') & (col("Temperature_Max_C") < 0),
        regexp_replace(col('Temperature_Max_C').cast("string"), '-', '').cast("double")).otherwise(col("Temperature_Min_C")))

# df_spark.show()