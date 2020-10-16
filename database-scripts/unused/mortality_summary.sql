SELECT MIN(date) AS earliest_date,
       MAX(date) AS lastest_date,
       COUNT(DISTINCT date) AS nb_days,
       COUNT(DISTINCT date)/365 AS nb_years
FROM mortality;

SELECT DATE_PART('year', mortality.date) AS year, COUNT(number)
FROM mortality
GROUP BY year
ORDER by year;
