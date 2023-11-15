#!/bin/bash

# Define the host and path for the tractor database
host="tractor-engine"
path="/var/spool/tractor/psql"

# Run df -H command to get disk space usage for the tractor database and store the output in a variable
df_output=$(ssh $host "df -H $path")

# Get the current date and time
timestamp=$(date "+%Y-%m-%d %H:%M:%S")

# Extract the used and available space from the df output
size=$(echo "$df_output" | awk '/\/dev\/sdb1/ {print $2}' | sed 's/G//')
used=$(echo "$df_output" | awk '/\/dev\/sdb1/ {print $3}' | sed 's/G//')
avail=$(echo "$df_output" | awk '/\/dev\/sdb1/ {print $4}' | sed 's/G//')

# Define the log file path and name
logfile="/mnt/w/PubLogs/render/tractor_db_size_$(date "+%m%d%Y").log"

# Write the output to the log file
echo "{\"timestamp\": \"$timestamp\", \"size\": $size, \"used\": $used, \"avail\": $avail}" >> $logfile
