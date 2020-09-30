ALTER TABLE stations ADD COLUMN geom geometry(Point, 26918);
UPDATE stations SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 26918);
