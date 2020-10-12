#!/bin/bash

# To be run on the PostgresQL instance

cd
mkdir c_10nv20
cd c_10nv20
wget https://www.weather.gov/source/gis/Shapefiles/County/c_10nv20.zip
unzip c_10nv20.zip

shp2pgsql -s 26918 ~/c_10nv20/c_10nv20.shp > c_10nv20.sql counties
psql -h localhost -d heatwave -U $POSTGRESQL_USER -f c_10nv20.sql
