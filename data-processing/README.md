# Data Processing

## Set up

Set up an Apache Spark cluster with 3 workers and the following software:
- Ubuntu 18.04 LTS: from an AWS EC2 Amazon Machine Image (AMI)
- Java: `sudo apt install openjdk-8-jre-headless` to install openjdk >= 1.7.4
- Scala: `sudo apt install scala` to install version 2.11.12
- Spark from `spark-2.4.7-bin-hadoop2.7.tgz`
- PostgreSQL JDBC driver: 
Download the driver as `jar` file with
`wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar` and 
place it in the home directory `~/`

## How to run 

### Import weather stations

1. Run `submit-import-stations.sh`, which calls `write_weather_stations.py`

### Import weather data

1. Modify the start and end year (end year IS included) in `submit-append-weather.sh`
by changing the 1st and 2nd parameters, respectively (e.g. 1968 2005)
1. Run `submit-append-weather.sh`, which calls `append_weather_data.py`

### Import mortality data

1. If adding mortality data outside the range 1968 - 2005,
edit the schema descriptor file `mort_schema.json`.
1. Modify the start and end year (end year IS included) in `submit-append-mortality.sh`
by changing the 1st and 2nd parameters, respectively (e.g. 1968 1988)
1. Run `submit-append-mort.sh`, which calls `append_mortality_data.py`

### Incremental addition
Both weather and mortality scripts can be called to incrementally append data to the database.
Please note than adding the same data year more than once will NOT return an error.


