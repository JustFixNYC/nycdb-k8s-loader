FROM python:3.6 AS base

RUN apt-get update && \
  apt-get install -y \
    unzip \
    postgresql-client && \
  rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install -r requirements.txt

ARG NYCDB_REPO=https://github.com/nycdb/nycdb
ARG NYCDB_REV=be13215c3b2368a52a62dbfd027d7cc8d76f75cb

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
ARG WOW_REV=96782e3967f5fd5e556152109922a628d624e065
RUN curl -L ${WOW_REPO}/archive/${WOW_REV}.zip > wow.zip \
  && unzip wow.zip \
  && rm wow.zip \
  && mv who-owns-what-${WOW_REV} who-owns-what

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
