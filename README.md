# heatwave
Insight Data Engineering Fellowship project

Main website:
https://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.ncdc:C00861

Data readme:
https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt

Daily data:
https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/


Tech stack installation

PostGIS on Ubuntu 18.04
https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-18-04

From: https://postgis.net/source/
'wget https://download.osgeo.org/postgis/source/postgis-3.0.2.tar.gz'

From

///

https://computingforgeeks.com/install-postgresql-12-on-ubuntu/
https://computingforgeeks.com/how-to-install-postgis-on-ubuntu-debian/

Update system:\
`sudo apt update` \
`sudo apt -y install vim bash-completion wget` \
`sudo apt -y upgrade`

`sudo reboot`

`wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -`

`echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list`

`sudo apt update`
`sudo apt -y install postgresql-12 postgresql-client-12`