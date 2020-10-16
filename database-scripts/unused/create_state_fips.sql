ALTER TABLE mortality
ADD COLUMN state_fips integer;

UPDATE mortality
SET state_fips = CAST(state as int);
