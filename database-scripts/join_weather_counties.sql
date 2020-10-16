--               Table "public.weather_mo"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- station     | text    |           |          |
-- date_mo     | date    |           |          |
-- measurement | text    |           |          |
-- value       | numeric |           |          |

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