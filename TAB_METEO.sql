-- Creazione della tabella 'Citta' per contenere l'elenco delle città, esempio per avere un riferimento ad un altra tabella con la chiave esterna
CREATE TABLE CITTA (
    NomeCitta VARCHAR(100) PRIMARY KEY,
    Regione VARCHAR(100),
    Paese VARCHAR(100)
);

-- Creazione della tabella 'REPORT_METEO_SEDI' con chiave primaria composta e chiave esterna
CREATE TABLE REPORT_METEO_SEDI (
    City VARCHAR(100),                  
    Temperature_C DECIMAL(5, 2),         
    Temperature_Max_C DECIMAL(5, 2),     
    Weather_description VARCHAR(255),   
    Humidity_percent INT,                
    Wind_speed_m_s DECIMAL(4, 2),       
    Time DATETIME,                       
    
    PRIMARY KEY (City, Time),            -- Chiave primaria composta da 'City' e 'Time'
    FOREIGN KEY (City) REFERENCES Citta(NomeCitta) -- Chiave esterna che fa riferimento alla tabella 'Citta' se necessaria
);

-- QUERY SQL

--numero codizione meteo distinte per paese

SELECT paese, count(DISTINCT condizioni_meteo) as condizioni
from tab_meteo
left join report_meteo_sedi on citta.NomeCitta = report_nome_sedi.citta
GROUP BY paese


-- How many distinct weather conditions were observed (rain/snow/clear/...) in a certain period?
SELECT COUNT(DISTINCT Weather_description) AS DistinctWeatherConditions
FROM REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59';


-- Rank the most common weather conditions in a certain period of time per city?
SELECT 
    City,
    Weather_description,
    COUNT(*) AS Frequency,
    RANK() OVER(PARTITION BY City ORDER BY COUNT(*) DESC) AS Rank
FROM  REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59'
GROUP BY City, Weather_description
ORDER BY City, Rank;


-- What are the temperature averages observed in a certain period per city?
SELECT City, AVG(Temperature_C) AS AvgTemperature
FROM REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59'
GROUP BY City;


-- What city had the highest absolute temperature in a certain period of time?
SELECT City, MAX(Temperature_Max_C) AS HighestTemperature
FROM  REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59'
GROUP BY City
ORDER BY HighestTemperature DESC
LIMIT 1;


-- Which city had the highest daily temperature variation in a certain period of time?
SELECT City, DATE(Time) AS Date, MAX(Temperature_C) - MIN(Temperature_C) AS DailyTemperatureVariation
FROM REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59'  -- Estendere l'intervallo per coprire le 24 ore
GROUP BY City, DATE(Time)
ORDER BY DailyTemperatureVariation DESC
LIMIT 1;


-- What city had the strongest wing in a certain period of time?
SELECT City, MAX(Wind_speed_m_s) AS HighestWindSpeed
FROM  REPORT_METEO_SEDI
WHERE Time BETWEEN '2024-10-24 00:00:00' AND '2024-10-26 23:59:59'
GROUP BY City
ORDER BY HighestWindSpeed DESC
LIMIT 1;



