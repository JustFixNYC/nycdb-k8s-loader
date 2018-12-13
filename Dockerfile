FROM python:3.6

RUN apt-get update \
  && apt-get install -y \
    unzip \
    postgresql-client \
  && rm -rf /var/lib/apt/lists/* \
  && rm -rf /src/*.deb

COPY requirements.txt /

ARG NYCDB_REPO=https://github.com/aepyornis/nyc-db
ARG NYCDB_REV=ac5f13cc80662efb3debd8d4514a6a7a99d4e05f

# We need to retrieve the source directly from the repository
# because we need access to the test data, which isn't part of
# the pypi distribution.
RUN curl -L ${NYCDB_REPO}/archive/${NYCDB_REV}.zip > nyc-db.zip \
  && unzip nyc-db.zip \
  && rm nyc-db.zip \
  && mv nyc-db-${NYCDB_REV} nyc-db \
  && cd nyc-db/src \
  && pip install -e .

RUN pip install -r requirements.txt

COPY . /app

WORKDIR /app
