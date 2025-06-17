FROM python:3.10 AS base

RUN apt-get update && \
  apt-get install -y \
  unzip \
  libpq5 \
  postgresql-client \
  postgis && \
  rm -rf /var/lib/apt/lists/*


# Setup geosupport for standardizing addresses for wow
# check the latest version here https://www.nyc.gov/site/planning/data-maps/open-data/dwn-gdelx.page
# make sure this gets updated regularly, and matches the version in who-owns-what
ENV RELEASE=25b
ENV MAJOR=25
ENV MINOR=2
ENV PATCH=0
WORKDIR /geosupport

RUN FILE_NAME=linux_geo${RELEASE}_${MAJOR}.${MINOR}.zip; \
    echo ${FILE_NAME}; \
    curl -O https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/geosupport/$FILE_NAME; \
    unzip *.zip; \
    rm *.zip;

ENV GEOFILES=/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

WORKDIR /

RUN python -m pip install pip==23.2

COPY requirements.txt /
RUN pip install -r requirements.txt

ARG NYCDB_REPO=https://github.com/nycdb/nycdb
ARG NYCDB_REV=ad0366b98b2c20b1b9a49b91e3edcced74146645
# We need to retrieve the source directly from the repository
# because we need access to the test data, which isn't part of
# the pypi distribution.
RUN curl -L ${NYCDB_REPO}/archive/${NYCDB_REV}.zip > nycdb.zip \
  && unzip nycdb.zip \
  && rm nycdb.zip \
  && mv nycdb-${NYCDB_REV} nycdb \
  && cd nycdb/src \
  && pip install .

ARG WOW_REPO=https://github.com/justFixNYC/who-owns-what
ARG WOW_REV=ab9491e56bfdf4922e0c73bbbd5215ff8de5647d
RUN curl -L ${WOW_REPO}/archive/${WOW_REV}.zip > wow.zip \
  && unzip wow.zip \
  && rm wow.zip \
  && mv who-owns-what-${WOW_REV} who-owns-what

# This is kind of terrible. We've got a package in WoW that
# generates our portfolio graphs, but it's not really made
# for distribution right now (e.g. it has no `setup.py` or
# formal list of dependencies). So here we're just "hacking"
# it into our Python installation and manually installing its
# dependencies, at least until we formally turn it into a
# real Python package.
RUN ln -s /who-owns-what/portfoliograph /usr/local/lib/python3.10/site-packages/portfoliograph && \
  pip install networkx==3.3 && \
  pip install numpy==2.0.1 && \
  pip install python-geosupport==1.0.8

# For now we also do the same process for OCA data prep.
RUN ln -s /who-owns-what/ocaevictions /usr/local/lib/python3.10/site-packages/ocaevictions && \
  pip install boto3==1.28.44

# And again for signature dashboard...
RUN ln -s /who-owns-what/signature /usr/local/lib/python3.10/site-packages/signature && \
  pip install boto3==1.28.44

# And again for good cause eviction...
RUN ln -s /who-owns-what/goodcause /usr/local/lib/python3.10/site-packages/goodcause

ENV PYTHONUNBUFFERED yup

# Note that these won't actually work until we either mount /app as a
# volume or copy it over. For dev, this will be done via volume mount
# by docker-compose; for prod the directory contents will be copied over
# in that stage.
WORKDIR /app
CMD ["python", "load_dataset.py"]


# Development container
FROM base AS dev
COPY requirements.dev.txt /
RUN pip install -r /requirements.dev.txt


# Production container
FROM base AS prod
COPY . /app
