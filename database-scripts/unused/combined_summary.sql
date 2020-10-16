SELECT MIN(date) AS earliest_date,
       MAX(date) AS lastest_date,
       COUNT(DISTINCT date) AS nb_days,
       COUNT(DISTINCT date)/365 AS nb_years
FROM combined;

SELECT DATE_PART('year', combined.date) AS year,
       COUNT(DISTINCT countyname) AS "County",
       COUNT(avg_value) AS "Available data points"
FROM combined
GROUP BY year
ORDER by year;
