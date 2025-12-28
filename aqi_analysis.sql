CREATE DATABASE aqi_analysis;
USE aqi_analysis;

CREATE TABLE aqi (
    year INT,
    month VARCHAR(50),
    state VARCHAR(100),
    district VARCHAR(100),
    number_of_monitoring_stations INT,
    is_pm25 TINYINT,
    is_pm10 TINYINT,
    is_no2 TINYINT,
    is_so2 TINYINT,
    is_co TINYINT,
    is_o3 TINYINT,
    aqi_value INT,
    air_quality_status VARCHAR(50)
);

DROP TABLE IF EXISTS disease;
CREATE TABLE disease (
    year INT,
    state VARCHAR(100),
    illness_category VARCHAR(100),
    cases INT,
    deaths INT
);

CREATE TABLE vehicle (
    year INT,
    month VARCHAR(50),
    state VARCHAR(100),
    vehicle_type VARCHAR(50),
    fuel_type VARCHAR(50),
    value INT
);

CREATE TABLE population (
    year INT,
    month VARCHAR(50),
    state VARCHAR(100),
    gender VARCHAR(10),
    value INT
);

LOAD DATA INFILE
'***/aqi.csv'
INTO TABLE aqi
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE
'***/population.csv'
INTO TABLE population
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE
'***Uploads/vehicle.csv'
INTO TABLE vehicle
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM aqi LIMIT 5;
SELECT * FROM disease LIMIT 5;
SELECT * FROM vehicle LIMIT 5;
SELECT * FROM population LIMIT 5;

#TOP 5 STATES WITH WORST AQIs
SELECT state,AVG(aqi_value) AS avg_aqi
FROM aqi 
GROUP BY state
ORDER BY AVG(aqi_value) DESC 
LIMIT 5;

#TOP 5 STATES WITH BEST AQIs
SELECT state,AVG(aqi_value) AS avg_aqi
FROM aqi 
GROUP BY state
ORDER BY AVG(aqi_value) 
LIMIT 5;

#months with worst aqi
SELECT month,ROUND(AVG(aqi_value),2) AS avg_aqi
FROM aqi 
GROUP BY month
ORDER BY AVG(aqi_value) DESC
LIMIT 3;

#months with best aqi
SELECT month,ROUND(AVG(aqi_value),2) AS avg_aqi
FROM aqi 
GROUP BY month
ORDER BY AVG(aqi_value) 
LIMIT 3;

#What are the dominant pollutants by state?
SELECT state,SUM(is_pm25),SUM(is_pm10),SUM(is_no2),SUM(is_so2),SUM(is_co),SUM(is_o3)
FROM aqi 
GROUP BY state
ORDER BY SUM(is_pm25)DESC,SUM(is_pm10)DESC,SUM(is_no2)DESC,
		 SUM(is_so2)DESC,SUM(is_co)DESC,SUM(is_o3)DESC;
#particulate matters impacts the most

#How often does air quality fall into unhealthy categories?
SELECT air_quality_status, COUNT(*) AS days
FROM aqi
GROUP BY air_quality_status
ORDER BY COUNT(*) DESC;

#Which illnesses are most reported across states?
SELECT illness_category, SUM(cases)
FROM disease
GROUP BY illness_category
ORDER BY SUM(cases) DESC;


#Does poor AQI align with higher disease burden?
SELECT state,AVG(aqi_value) AS avg_aqi
FROM aqi 
GROUP BY state
ORDER BY AVG(aqi_value) DESC;

SELECT state,SUM(cases) AS total_cases
FROM disease
GROUP BY state
ORDER BY SUM(cases) DESC;

#How does vehicle volume vary across states?
SELECT state, SUM(value) AS total_vehicles
FROM vehicle
GROUP BY state
ORDER BY total_vehicles DESC;

#How does disease burden scale with population?
SELECT d.state, d.year,
       SUM(d.cases) / SUM(p.value) * 100000 AS cases_per_lakh
FROM disease d
JOIN population p
  ON d.state = p.state AND d.year = p.year
WHERE p.gender = 'Total' and d.year between 2020 AND 2025
GROUP BY d.state, d.year
ORDER BY SUM(d.cases) / SUM(p.value) * 100000 DESC;

