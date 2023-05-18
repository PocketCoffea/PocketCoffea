#!/bin/bash

# exit when any command fails; be verbose
set -e
pwd

echo "======================================="
echo "Installing PocketCoffea"
echo "on cluster environment: "$1
echo "Current time:" $(date)
echo "======================================="

echo "Fixing dependencies in the image"
if [[ $1 == "lxplus" ]]; then

    pip install --upgrade dask-lxplus
fi

echo "Installing PocketCoffea"

pip3 install .
