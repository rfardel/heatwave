--
--                    Table "public.weatherbycounty"
--   Column    |         Type          | Collation | Nullable | Default
---------------+-----------------------+-----------+----------+---------
-- state       | text                  |           |          |
-- state_fips  | integer               |           |          |
-- county_name | character varying(50) |           |          |
-- county_fips | integer               |           |          |
-- date_mo     | date                  |           |          |
-- avg_value   | numeric(9,2)          |           |          |
--
----
--              Table "public.mortality_mo"
--   Column    |  Type   | Collation | Nullable | Default
---------------+---------+-----------+----------+---------
-- state       | integer |           |          |
-- county_fips | integer |           |          |
-- date_mo     | date    |           |          |
-- number_mo   | numeric |           |          |



CREATE TABLE combined_mo AS (
SELECT weatherbycounty.date_mo AS agg_date,
       weatherbycounty.state,
       weatherbycounty.county_name,
       weatherbycounty.avg_value,
       mortality_mo.number_mo as sum_mort
FROM weatherbycounty

JOIN mortality_mo
ON (weatherbycounty.state_fips = mortality_mo.state AND
    weatherbycounty.county_fips = mortality_mo.county_fips AND
    weatherbycounty.date_mo = mortality_mo.date_mo
   )

ORDER BY agg_date,
         weatherbycounty.state,
         weatherbycounty.county_name
);