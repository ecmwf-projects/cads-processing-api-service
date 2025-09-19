FROM condaforge/miniforge3:23.11.0-0

ARG MODE=stable
ARG CADS_PAT

WORKDIR /src

COPY ./git-*-repos.py /src/

COPY environment.${MODE} /src/environment
COPY environment-common.yml /src/environment-common.yml
COPY . /src/cads-processing-api-service

RUN conda install -y -n base -c conda-forge gitpython typer conda-merge

SHELL ["/bin/bash", "-c"]

RUN set -a && source environment \
    && CADS_PAT=${CADS_PAT} python ./git-clone-repos.py --default-branch \
    cacholote \
    cads-adaptors \
    cads-broker \
    cads-catalogue \
    cads-common \
    ogc-api-processes-fastapi

# NOTE: no environment for cads-adaptors as we only use basic features
RUN conda run -n base conda-merge \
    /src/environment-common.yml \
    /src/cacholote/environment.yml \
    /src/cads-broker/environment.yml \
    /src/cads-catalogue/environment.yml \
    /src/cads-common/environment.yml \
    /src/cads-processing-api-service/environment.yml \
    /src/ogc-api-processes-fastapi/environment.yml \
    > /src/combined-environment.yml \
    && conda env update -n base -f /src/combined-environment.yml \
    && conda clean -afy

RUN conda run -n base pip install --no-deps \
    -e /src/cacholote \
    -e /src/cads-broker \
    -e /src/cads-catalogue \
    -e /src/cads-common \
    -e /src/cads-processing-api-service \
    -e /src/ogc-api-processes-fastapi

# NOTE: pip install cads-adaptors mandatory dependencies
RUN conda run -n base pip install -e /src/cads-adaptors

CMD uvicorn cads_processing_api_service.main:app --host 0.0.0.0 --log-level info
