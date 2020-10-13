CREATE TABLE combined AS (
SELECT stacoun.state,
       --stacoun.state_fips,
       stacoun.county_name AS county,
       --stacoun.county_fips,
       --count(stacoun.name) as nb_stations,
       --weather.date,
       --weather.measurement,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avg_value,
       SUM(mortality.number) as sum_mort,
       date_trunc('month', mortality.date) AS agg_date
FROM weather

JOIN stacoun
ON weather.station = stacoun.station_id

JOIN mortality
ON (mortality.county_fips = stacoun.county_fips
    AND mortality.state = stacoun.state_fips
    AND date_trunc('month', mortality.date) = date_trunc('month', weather.date)
   )

GROUP BY stacoun.state,
         --stacoun.state_fips,
         county,
         --stacoun.county_fips,
         agg_date
         --weather.measurement,
         --mortality.date
ORDER BY agg_date,
         --nb_stations DESC,
         stacoun.state,
         county
         --weather.measurement
);