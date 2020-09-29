#!/bin/sh
# Source: https://computingforgeeks.com/how-to-install-postgis-on-ubuntu-debian/
# Accessed on 2020-09-22
# Part 1 of 2 - before reboot

# Update system
sudo apt update
sudo apt -y install vim bash-completion wget
sudo apt -y upgrade

# Reboot required
sudo reboot