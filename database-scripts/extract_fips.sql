ALTER TABLE counties
ADD COLUMN cfips integer;

UPDATE counties
SET cfips = CAST(RIGHT(fips, 3) as int);