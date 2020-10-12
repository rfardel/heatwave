CREATE TABLE stacoun AS (
SELECT stations.state,
       counties.sfips AS state_fips,
       counties.county AS county_name,
       counties.cfips AS county_fips,
       --stations.name,
       count(stations.name) as nb_stations
FROM stations

JOIN counties
ON (ST_Contains(counties.geom, stations.geom) AND
    stations.state = counties.state)

WHERE stations.station_id LIKE 'US%'
      --AND weather.date BETWEEN '2003-05-01' AND '2003-05-10'
      --AND counties.nhgisnam LIKE 'Los A%'
      --AND stations.state LIKE 'CA'
GROUP BY stations.state,
         counties.sfips,
         counties.cfips,
         counties.county

ORDER BY stations.state,
         counties.county
);