--                 Table "public.weather"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- station     | text    |           |          |
-- date        | date    |           |          |
-- measurement | text    |           |          |
-- value       | integer |           |          |
--

CREATE TABLE weather_mo AS (
SELECT station,
       CAST(date_trunc('month', weather.date) AS date) AS date_mo,
       measurement,
       AVG(value) AS value
FROM weather
GROUP BY station, date_mo, measurement
ORDER BY station, date_mo, measurement
);