ALTER TABLE counties
ADD COLUMN cfips integer;

ALTER TABLE counties
ADD COLUMN sfips integer;

UPDATE counties
SET cfips = CAST(LEFT(county, 3) as int);

UPDATE counties
SET sfips = CAST(LEFT(state, 2) as int);