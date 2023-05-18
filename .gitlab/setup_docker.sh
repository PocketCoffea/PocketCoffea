#!/bin/bash

# exit when any command fails; be verbose
set -e
pwd

echo "======================================="
echo "Installing PocketCoffea"
echo "Current time:" $(date)
echo "======================================="

pip3 install . 
