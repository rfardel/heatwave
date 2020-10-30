CREATE TABLE weatherbycounty AS (
SELECT stacoun.state,
       stacoun.state_fips,
       stacoun.county_name,
       stacoun.county_fips,
       weather_mo.date_mo,
       CAST(AVG(weather_mo.value)/10 AS DECIMAL(9,2)) AS avg_value
FROM weather_mo

JOIN stacoun
ON weather_mo.station = stacoun.station_id

GROUP BY stacoun.state,
         stacoun.state_fips,
         stacoun.county_name,
         stacoun.county_fips,
         weather_mo.date_mo

ORDER BY stacoun.state_fips,
         stacoun.county_fips,
         weather_mo.date_mo
);