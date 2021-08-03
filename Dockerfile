FROM python:3.6.14 AS base

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
ARG NYCDB_REV=1cccea098a395f5070a5589b6d13ab3cf7d1fa91

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
ARG WOW_REV=cc22c2aee8581e6fb9e3a78a1f29b0cabf6898eb
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
