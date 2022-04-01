FROM python:3.6 AS base

RUN apt-get update && \
  apt-get install -y \
  unzip \
  postgresql-client && \
  rm -rf /var/lib/apt/lists/*

# The latest version of pip at the time of this writing, 20.3, results
# in an infinite loop of "Requirement already satisfied" when
# installing our dependencies, so we're forcibly downgrading to
# the most recent version that works.
RUN python -m pip install pip==20.2.4

COPY requirements.txt /
RUN pip install -r requirements.txt

ARG NYCDB_REPO=https://github.com/nycdb/nycdb
ARG NYCDB_REV=34d1bf7b9a630d987b1f18cf02b78004df1e3aa0
# We need to retrieve the source directly from the repository
# because we need access to the test data, which isn't part of
# the pypi distribution.
RUN curl -L ${NYCDB_REPO}/archive/${NYCDB_REV}.zip > nycdb.zip \
  && unzip nycdb.zip \
  && rm nycdb.zip \
  && mv nycdb-${NYCDB_REV} nycdb \
  && cd nycdb/src \
  && pip install -e .

ARG WOW_REPO=https://github.com/justFixNYC/who-owns-what
ARG WOW_REV=fd708d3f2dd99ffbfc75cadf729e2e66ea4cf356
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
RUN ln -s /who-owns-what/portfoliograph /usr/local/lib/python3.6/site-packages/portfoliograph && \
  pip install networkx==2.5.1 && pip install numpy==1.19.5

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
