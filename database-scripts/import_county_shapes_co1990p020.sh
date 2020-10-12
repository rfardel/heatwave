#!/bin/bash

# To be run on the PostgresQL instance

shp2pgsql -s 4269 co1990p020.shp > co1990p020.sql counties
psql -h localhost -d heatwave -U $POSTGRESQL_USER -f co1990p020.sql
