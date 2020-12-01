#!/bin/bash
cd /content/drive/MyDrive/modovision/NYC_Taxi
mkdir -p /content/drive/MyDrive/modovision/NYC_Taxi/green_taxi
mkdir -p /content/drive/MyDrive/modovision/NYC_Taxi/yellow_taxi
# Download green taxi dataset 
wget https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_20{13..16}-{01..12}.csv -p /content/drive/MyDrive/modovision/NYC_Taxi/green_taxi
# Download yellow taxi dataset 
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_20{09..16}-{01..12}.csv -p /content/drive/MyDrive/modovision/NYC_Taxi/yellow_taxi