#!/bin/bash

CPUIDS=$(seq 20 1 39)

for ID in $CPUIDS; do
    echo 1 | sudo tee /sys/devices/system/cpu/cpu$ID/online
done
