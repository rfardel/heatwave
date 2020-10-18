# Heatwave
_Protecting populations from extreme temperatures_

By Romain Fardel

A project created for the 
[Insight Data Engineering Fellowship](https://insightfellows.com/), 
Fall 2020 session (20C)

## Introduction

Heatwave can be deadly for populations. For instance, 
over 30,000 people died from the heatwave that swept Europe 
during the summer 2003. The issue affects the whole world, 
and the problem will keep increasing with global warming. 

To protect populations, researchers need to study 
how mortality is affected by temperature. 
The problem is that there is currently not a single source of 
data available to researchers. 
For the US, researchers need to retrieve vital statistics 
from the CDC and historical weather data from NOAA. 
Combining these datasets in challenging for a few reasons:

- **Geographical mismatch**: mortality is reported by county, 
whereas temperature is reported by weather station.

- **Temporal mismatch**: mortality is reported by day or by month, 
where temperature is reported at variable intervals depending 
on the data year and station location.

- **Schema evolution**: the format of mortality data evolves every few years, 
and data is provided in a non-delimited format, 
where knowledge of the position of each field is needed to extract the data.

- **County evolution in time**: county limits have changed over the last 50 years.

This project addresses that need by creating a pipeline 
to combine these datasets and make them available in a GIS-enabled (Geographic Information System) database 
that the end user can query with SQL.

## Execution

![Tech stack](./assets/tech_stack.png)

1. Raw data is stored in Amazon S3
    - Weather data is readily available in a NOAA S3 bucket
    - Mortality data is ingested by downloading from FTP, unzipping and saving 
    text files to S3
1. Raw data is processed in Apache Spark. Each dataset is extracted, 
filtered, and aggregated separately
1. Data is loaded to PostgreSQL with the PostGIS extension
1. Auxillary datasets (weather station, county definitions) are loaded to PostGIS and the data is joined
1. Final table is queried on demand and displayed in Dash.

## Data sources

### Weather
[NOAA Global Historical Climatology Network Daily (GHCN-D)](https://registry.opendata.aws/noaa-ghcn/), 
available in an Amazon S3 bucket.

### Mortality

#### Data files
[CDC - Vital Statistics Online Data Portal](https://www.cdc.gov/nchs/data_access/vitalstatsonline.htm), 
under _Mortality Multiple Cause Files_, U.S. data (.zip files).

#### Descriptor files
[CDC - Public Use Data File Documentation](https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm),
in PDF format.

### Weather stations
`ghcnd-stations.txt` from NOAA bucket

### Counties

[County Boundaries of the United States, 1990](https://geodata.lib.berkeley.edu/catalog/stanford-pb817xw6983)

### County time concordance (a.k.a. crosswalk)

A Crosswalk for US Spatial Data 1790 - 2000, Fabian Eckert and [Michael Peters](https://mipeters.weebly.com/research.html).
[ZIP file](https://mipeters.weebly.com/uploads/1/4/6/5/14651240/egp_crosswalk.zip)