ALTER TABLE counties
ADD COLUMN cfips integer;

ALTER TABLE counties
ADD COLUMN sfips integer;

UPDATE counties
SET cfips = CAST(RIGHT(fips, 3) as int);

UPDATE counties
SET sfips = CAST(LEFT(fips, 2) as int);