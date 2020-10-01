SELECT stations.state,
       counties.countyname,
       count(stations.name) as nb_sta,
       weather.date,
       weather.measurement,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avgvalue
FROM weather

LEFT JOIN stations
ON weather.station = stations.station_id

JOIN counties
ON (ST_Contains(counties.geom, stations.geom)
    AND stations.state = counties.state)

WHERE (weather.date >= 20030501) AND (weather.date <= 20030531)
    AND weather.station LIKE 'US%'
GROUP BY stations.state, counties.countyname, weather.date, weather.measurement
ORDER BY nb_sta DESC, stations.state, counties.countyname, weather.date, weather.measurement;