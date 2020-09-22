#!/bin/sh
# Source: https://computingforgeeks.com/how-to-install-postgis-on-ubuntu-debian/
# Accessed on 2020-09-22
# Part 2 of 2 - after reboot

# Prep
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list
sudo apt update

# Install Postgres
sudo apt -y install postgresql-12 postgresql-client-12

# Install PostGIG
sudo apt install postgis postgresql-12-postgis-3