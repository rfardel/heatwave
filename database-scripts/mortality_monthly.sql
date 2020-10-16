--heatwave=# \d mortality
--                Table "public.mortality"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- state       | integer |           |          |
-- county_fips | integer |           |          |
-- date        | date    |           |          |
-- number      | bigint  |           |          |



CREATE TABLE mortality_mo AS (
SELECT state,
       county_fips,
       CAST(date_trunc('month', mortality.date) AS date) AS date_mo,
       SUM(number) AS number_mo
FROM mortality
GROUP BY state, county_fips, date_mo
ORDER BY state, county_fips, date_mo
);
