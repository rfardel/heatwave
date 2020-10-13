CREATE TABLE stacoun AS (
SELECT stations.station_id,
       stations.state,
       counties.county AS county_name,
       counties.sfips AS state_fips,
       counties.cfips AS county_fips,
       stations.name
FROM stations

JOIN counties
ON (ST_Contains(counties.geom, stations.geom) AND
    stations.state = counties.state)

WHERE stations.station_id LIKE 'US%'

ORDER BY stations.state,
         counties.county,
         stations.name
);