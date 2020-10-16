CREATE TABLE weather_fips AS (
select
weather.station,
weather.date,
weather.measurement,
weather.value,
stacoun.state_fips,
stacoun.county_fips

from weather

join stacoun on stacoun.station_id = weather.station
);