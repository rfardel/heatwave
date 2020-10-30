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