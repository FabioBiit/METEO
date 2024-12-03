import requests
import pandas as pd
# import com.microsoft.spark.sqlanalytics
from datetime import datetime

data_time_stamp = datetime.now().replace(microsecond=0)

# print(data_time_stamp)

# milano = "https://api.openweathermap.org/data/2.5/weather?q=Milano&appid=d772ae99953d3504ba931841f8bd77da&units=metric"
# bologna = "https://api.openweathermap.org/data/2.5/weather?q=Bologna&appid=d772ae99953d3504ba931841f8bd77da&units=metric"
# cagliari = "https://api.openweathermap.org/data/2.5/weather?q=Cagliari&appid=d772ae99953d3504ba931841f8bd77da&units=metric"

citta = ["milano", "bologna", "cagliari"]

dataframe = {} # Diz per lo storage dei 3 dataframe

for i, city in enumerate(citta):

    citta2 = "https://api.openweathermap.org/data/2.5/weather?q="+city+"&appid=d772ae99953d3504ba931841f8bd77da&units=metric"

    response = requests.get(citta2)

    # Verificare che la richiesta sia andata a buon fine
    if response.status_code == 200:
        # Ottenere i dati in formato JSON
        data = response.json()
        
        # Estraggo i dati ritenuti rilevanti
        dati = {
            'City' :  data['name'],
            'Temperature_C°' : data['main']['temp'],
            'Temperature_Max_C°' : data['main']['temp_max'],
            'Weather_description' : data['weather'][0]['description'],
            'Humidity_%' : data['main']['humidity'],
            'Wind_speed_m/s' : data['wind']['speed'],
            'Time' : data_time_stamp
        }
    
        df_pd = pd.DataFrame([dati])

        dataframe[f"df_{i+1}"] = df_pd

    else:
        print("Failed to retrieve data from the API. Status code:", response.status_code)

df_final = pd.concat(dataframe, ignore_index=True).drop_duplicates() # Unione dei 3 dataframe creati in precedenza in un unico DF

df_final['City'] = df_final['City'].replace('Provincia di Cagliari', 'Cagliari') # Rinomino un valore per una migliore lettura

print(df_final)

# Metodo per salvare in append su una tabella nel DB SQL di AZURE SYNAPSE
# df_final.write.mode("append").synapsesql("nome_tab_SQL")

# df_final.to_csv("C:/Users/kyros/OneDrive/Desktop/dataframe_meteo_2.csv", mode='a', index=False, header=False) # Salvo il DF come CSV per simulare il save su DB

# df_final.info()


