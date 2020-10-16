--                    Table "public.weatherbycounty"
--   Column    |         Type          | Collation | Nullable | Default
---------------+-----------------------+-----------+----------+---------
-- state       | text                  |           |          |
-- state_fips  | integer               |           |          |
-- county_name | character varying(50) |           |          |
-- county_fips | integer               |           |          |
-- date        | date                  |           |          |
-- avg_value   | numeric(9,2)          |           |          |
--
--                Table "public.mortality"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- state       | integer |           |          |
-- county_fips | integer |           |          |
-- date        | date    |           |          |
-- number      | bigint  |           |          |


CREATE TABLE combined_mo AS (
SELECT CAST(date_trunc('month', weatherbycounty.date) AS date) AS agg_date,
       weatherbycounty.state,
       weatherbycounty.county_name,
       CAST(AVG(weatherbycounty.avg_value)/10 AS DECIMAL(9,2)) AS avg_value,
       SUM(mortality.number) as sum_mort
FROM weatherbycounty

JOIN mortality
ON (weatherbycounty.state_fips = mortality.state AND
    weatherbycounty.county_fips = mortality.county_fips AND
    date_trunc('month', weatherbycounty.date) = date_trunc('month', mortality.date)
   )

GROUP BY agg_date,
         weatherbycounty.state,
         weatherbycounty.county_name

ORDER BY agg_date,
         weatherbycounty.state,
         weatherbycounty.county_name
);