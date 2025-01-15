import requests
import pandas as pd
from datetime import datetime



data_time_stamp = datetime.now().replace(microsecond=0)

citta = ["milano", "bologna", "cagliari", "palermo", "napoli"]

dataframe = {} # Diz per lo storage dei 3 dataframe

for i, city in enumerate(citta):

    citta_str = "https://api.openweathermap.org/data/2.5/weather?q="+city+"&appid=d772ae99953d3504ba931841f8bd77da&units=metric"

    response = requests.get(citta_str)

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

df_final['City'] =  df_final['City'].replace('Provincia di Cagliari', 'Cagliari')\
                                    .replace('Province of Palermo', 'Palermo') # Rinomino valori per una migliore lettura

print(df_final)

# Metodo per salvare in append su una tabella nel DB SQL di AZURE SYNAPSE
# df_final.write.mode("append").synapsesql("nome_tab_SQL")

df_final.to_csv("C:/Users/kyros/OneDrive/Desktop/METEO/STORICO/DataFrameMeteo.csv", mode='a', index=False, header=False) # Salvo il DF come CSV per simulare il save su DB

# df_final.info()