SELECT weather.station,
       stations.station_id,
       stations.name,
       stations.state,
       CAST(stations.latitude AS DECIMAL(9,5)) AS lat,
       CAST(stations.longitude AS DECIMAL(9,5)) AS long,
       -- ST_X(stations.geom),  -- use geom for GIS data
       weather.date,
       weather.measurement,
       CAST(weather.value / 10.0 AS DECIMAL(9,1)) AS TavgC
FROM weather
LEFT JOIN stations
ON weather.station = stations.station_id
WHERE date = '20030510'
AND weather.station LIKE 'US%'
ORDER BY stations.state, TavgC;