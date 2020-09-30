SELECT name,
       state,
       --ST_AsText(geom),
       ST_Distance(stations.geom, ST_GeomFromText('POINT(-105.819999694824 40.4099998474121)', 4326)) as dist
FROM stations
WHERE name like 'La%'
ORDER BY dist;