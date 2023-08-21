ARG FROM_IMAGE=gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest
FROM ${FROM_IMAGE}

ARG CLUSTER=lxplus

ADD . .

RUN echo "=======================================" && \
    echo "Installing PocketCoffea" && \
    echo "on cluster environment: $CLUSTER" && \
    echo "Current time:" $(date) && \
    echo "=======================================" && \
    if [[ ${CLUSTER} == "lxplus" ]]; then \
        echo "Fixing dependencies in the image" && \
        conda install -y numba>=0.57.0 llvmlite==0.40.0 numpy>=1.22.0 && \
        pip install --upgrade dask-lxplus; \
    fi && \
    echo "Installing PocketCoffea" && \
    pip3 install .
