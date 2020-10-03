SELECT stations.state,
       counties.countyname,
       count(stations.name) as nb_stations,
       weather.date,
       weather.measurement,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avg_value,
       SUM(mortality.number) as sum_mort,
       mortality.date AS mort_date,
       mortality.weekday,
       mortality.manner
FROM weather

LEFT JOIN stations
ON weather.station = stations.station_id

JOIN counties
ON (ST_Contains(counties.geom, stations.geom)
    AND stations.state = counties.state)

JOIN mortality
ON (mortality.county_fips = counties.cfips
    AND counties.state = mortality.state
    AND date_trunc('month', mortality.date) = date_trunc('month', weather.date)
   )

WHERE weather.date BETWEEN '2003-05-01' AND '2003-05-10'
    AND weather.station LIKE 'US%'
    AND counties.countyname LIKE 'Los A%'
    AND stations.state LIKE 'CA'
GROUP BY stations.state,
         counties.countyname,
         weather.date,
         weather.measurement,
         mortality.date,
         mortality.weekday,
         mortality.manner
ORDER BY weather.date, nb_stations DESC, stations.state, counties.countyname, weather.measurement;