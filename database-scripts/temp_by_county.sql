CREATE TABLE combined AS (
SELECT stations.state,
       counties.countyname,
       count(stations.name) as nb_stations,
       weather.date,
       weather.measurement,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avg_value,
       SUM(mortality.number) as sum_mort,
       mortality.date AS mort_date
FROM weather

LEFT JOIN stations
ON weather.station = stations.station_id

JOIN counties
ON (ST_Contains(counties.geom, stations.geom)
    AND stations.state = counties.state)

JOIN mortality
ON (mortality.county_fips = counties.cfips
    AND mortality.state = counties.state
    AND date_trunc('day', mortality.date) = date_trunc('day', weather.date)
   )

WHERE weather.station LIKE 'US%'
      --AND weather.date BETWEEN '2003-05-01' AND '2003-05-10'
      --AND counties.countyname LIKE 'Los A%'
      --AND stations.state LIKE 'CA'
GROUP BY stations.state,
         counties.countyname,
         weather.date,
         weather.measurement,
         mortality.date
ORDER BY weather.date, nb_stations DESC, stations.state, counties.countyname, weather.measurement
);