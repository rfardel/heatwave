CREATE TABLE stacou AS
(SELECT stations.state,
        counties.countyname,
        counties.cfips,
        stations.name,
        stations.geom
       --ST_AsText(geom),
       --ST_DWithin(stations.geom, ST_GeomFromText('POINT(-105.819999694824 40.4099998474121)', 4326), 10) as nearby,
       --ST_Distance(stations.geom, ST_GeomFromText('POINT(-105.819999694824 40.4099998474121)', 4326)) as dist
       --counties.state,
FROM stations
JOIN counties
ON (ST_Contains(counties.geom, stations.geom) AND stations.state = counties.state)
--WHERE stations.state LIKE 'CO'
ORDER BY stations.state, counties.countyname, stations.name);