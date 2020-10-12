#!/bin/bash

# To be run on the PostgresQL instance

shp2pgsql -s 97456 1990_cz_v2.shp > 1990_cz_v2.sql counties
psql -h localhost -d heatwave -U $POSTGRESQL_USER -f 1990_cz_v2.sql
