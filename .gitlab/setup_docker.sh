#!/bin/bash

# exit when any command fails; be verbose
set -e
pwd

echo "======================================="
echo "Installing PocketCoffea"
echo "on cluster environment: "$1
echo "Current time:" $(date)
echo "======================================="

pip3 install .

if [[ $1 == "lxplus" ]]; then
    pip install --upgrade dask-lxplus
fi
