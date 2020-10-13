--
--                 Table "public.weather"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- station     | text    |           |          |
-- date        | date    |           |          |
-- measurement | text    |           |          |
-- value       | integer |           |          |
--
--
--
--                        Table "public.stacoun"
--   Column    |         Type          | Collation | Nullable | Default
---------------+-----------------------+-----------+----------+---------
-- station_id  | text                  |           |          |
-- state       | text                  |           |          |
-- county_name | character varying(50) |           |          |
-- state_fips  | integer               |           |          |
-- county_fips | integer               |           |          |
-- name        | text                  |           |          |


CREATE TABLE weatherbycounty AS (
SELECT stacoun.state,
       stacoun.state_fips,
       stacoun.county_name,
       stacoun.county_fips,
       weather.date,
       CAST(AVG(weather.value)/10 AS DECIMAL(9,2)) AS avg_value
FROM weather

JOIN stacoun
ON weather.station = stacoun.station_id

GROUP BY stacoun.state,
         stacoun.state_fips,
         stacoun.county_name,
         stacoun.county_fips,
         weather.date

ORDER BY stacoun.state_fips,
         stacoun.county_fips,
         weather.date
);