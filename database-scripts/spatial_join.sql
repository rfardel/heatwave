SELECT weather.station,
       stations.name,
       CAST(weather.value / 10.0 AS DECIMAL(9,1)) AS TavgC,
       mortality.number,
       counties.countyname
FROM weather
JOIN stations
ON stations.station_id = weather.station
JOIN counties
ON ST_Contains(counties.geom, stations.geom)
JOIN mortality
ON (mortality.fips = counties.cfips
    AND counties.state = mortality.state
   )
WHERE weather.station like 'US%' AND weather.date = '20030510'
LIMIT 100
;



/*SELECT weather.station,
       stations.station_id,
       ST_X(stations.geom),
       weather.date,
       CAST(weather.value / 10.0 AS DECIMAL(9,1)) AS TavgC
FROM weather
JOIN stations
ON weather.station = stations.station_id
WHERE (date = '20030510' AND weather.station LIKE 'US%')
JOIN (SELECT counties.state as cstate,
             mortality.state as mstate,
             counties.countyname,
             counties.cfips as cfips,
             mortality.fips as mfips,
             mortality.year,
             mortality.month,
             mortality.weekday,
             mortality.manner,
             mortality.number,
             counties.geom
      FROM mortality
      JOIN counties
      ON (mortality.fips = counties.cfips AND counties.state = mortality.state)
      WHERE counties.countyname LIKE 'Bi%'
      AND mortality.month = 5
      ORDER BY counties.countyname;
      ) AS mortcty
ON (ST_Contains(mortcty.geom, stations.geom)
AND stations.state = mortcty.cstate)
;*/

/*
ON
    (ST_Contains(counties.geom, stations.geom)
    AND stations.state = counties.state)
*/



/*SELECT weather.station,
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
ORDER BY stations.state, TavgC;*/

--
/* SELECT counties.state as cstate,
       mortality.state as mstate,
       counties.countyname,
       counties.cfips as cfips,
       mortality.fips as mfips,
       mortality.year,
       mortality.month,
       mortality.weekday,
       mortality.manner,
       mortality.number
FROM mortality
JOIN counties
ON (mortality.fips = counties.cfips AND counties.state = mortality.state)
WHERE counties.countyname like 'Bi%'
ORDER BY counties.countyname; */


--
/* CREATE TABLE stacou AS
(SELECT stations.state,
        counties.countyname,
        counties.cfips,
        stations.name AS stationname,
        stations.geom
       --ST_AsText(geom),
       --ST_DWithin(stations.geom, ST_GeomFromText('POINT(-105.819999694824 40.4099998474121)', 4326), 10) as nearby,
       --ST_Distance(stations.geom, ST_GeomFromText('POINT(-105.819999694824 40.4099998474121)', 4326)) as dist
       --counties.state,
FROM stations
JOIN counties
ON (ST_Contains(counties.geom, stations.geom) AND stations.state = counties.state)
--WHERE stations.state LIKE 'CO'
ORDER BY stations.state, counties.countyname, stations.name); */