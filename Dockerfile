ARG FROM_IMAGE=gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest
FROM ${FROM_IMAGE}

ARG CLUSTER=lxplus-cc7

ADD . .

RUN echo "=======================================" && \
    echo "Installing PocketCoffea" && \
    echo "on cluster environment: $CLUSTER" && \
    echo "Current time:" $(date) && \
    echo "=======================================" && \
    if [[ ${CLUSTER} == "lxplus-cc7" ]]; then \
        echo "Fixing dependencies in the image" && \
        conda install -y numba>=0.57.0 llvmlite==0.40.0 numpy>=1.22.0 && \
        python -m pip install -U dask-lxplus==0.3.2 dask-jobqueue==0.8.2; \
    elif [[ ${CLUSTER} == "lxplus-el9" ]]; then \
        echo "Installing on alma9"; \
    fi && \
    echo "Installing PocketCoffea" && \
    python -m pip install "setuptools<71" && \
    python -m pip install . --verbose
