SELECT stations.state,
       counties.countyname,
       count(stations.name) as nb_sta,
       weather.date,
       weather.measurement,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avgvalue,
       SUM(mortality.number) as sum_mort,
       mortality.month,
       mortality.weekday,
       mortality.manner
FROM weather

LEFT JOIN stations
ON weather.station = stations.station_id

JOIN counties
ON (ST_Contains(counties.geom, stations.geom)
    AND stations.state = counties.state)

JOIN mortality
ON (mortality.fips = counties.cfips
    AND counties.state = mortality.state
   )

WHERE weather.date BETWEEN '2003-05-01' AND '2003-05-10'
    AND weather.station LIKE 'US%'
GROUP BY stations.state,
         counties.countyname,
         weather.date,
         weather.measurement,
         mortality.month,
         mortality.weekday,
         mortality.manner
ORDER BY nb_sta DESC, stations.state, counties.countyname, weather.date, weather.measurement;