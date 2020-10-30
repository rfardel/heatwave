ALTER TABLE stations ADD COLUMN geom geometry(Point, 4269);
UPDATE stations SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4269);
