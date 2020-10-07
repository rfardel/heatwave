select mortality.date, mortality.state, mortality.county_fips, counties.countyname, mortality.number
from mortality
join counties
on (counties.state = mortality.state) AND (counties.cfips = mortality.county_fips)
order by mortality.number desc, mortality.date, mortality.state, mortality.county_fips;