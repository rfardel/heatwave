SELECT MIN(date) AS earliest_date,
       MAX(date) AS lastest_date,
       COUNT(DISTINCT date) AS nb_days,
       COUNT(DISTINCT date)/365 AS nb_years
FROM weather;

SELECT DATE_PART('year', weather.date) AS year, COUNT(value)
FROM weather
GROUP BY year
ORDER by year;
