SELECT counties.state as cstate,
       mortality.state as mstate,
       counties.countyname,
       counties.cfips as cfips,
       mortality.fips as mfips,
       mortality.year,
       mortality.month,
       mortality.weekday,
       mortality.manner,
       mortality.number
FROM mortality
JOIN counties
ON (mortality.fips = counties.cfips AND counties.state = mortality.state)
WHERE counties.countyname like 'Bi%'
ORDER BY counties.countyname;

/*
SELECT orders.order_id, suppliers.name
FROM suppliers
INNER JOIN orders
ON suppliers.supplier_id = orders.supplier_id
ORDER BY order_id;
*/
