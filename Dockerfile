FROM python:3.9 AS base

RUN apt-get update && \
  apt-get install -y \
  unzip \
  libpq5 \
  postgresql-client \
  postgis && \
  rm -rf /var/lib/apt/lists/*

RUN python -m pip install pip==23.2

COPY requirements.txt /
RUN pip install -r requirements.txt

ARG NYCDB_REPO=https://github.com/nycdb/nycdb
ARG NYCDB_REV=388d1f941834df1584e4024c5174580036b25634
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
ARG WOW_REV=db549f8f457e06bddb70fe69683ebaf3a31d047b
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
RUN ln -s /who-owns-what/portfoliograph /usr/local/lib/python3.9/site-packages/portfoliograph && \
  pip install networkx==2.5.1 && pip install numpy==1.19.5

# For now we also do the same process for OCA data prep.
RUN ln -s /who-owns-what/ocaevictions /usr/local/lib/python3.9/site-packages/ocaevictions && \
  pip install boto3==1.28.44

# And again for signature dashboard...
RUN ln -s /who-owns-what/signature /usr/local/lib/python3.9/site-packages/signature && \
  pip install boto3==1.28.44

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
