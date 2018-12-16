FROM python:3.6-alpine

COPY requirements* /

ARG REQUIREMENTS_FILE=requirements.txt

RUN apk update && \
  apk add postgresql-client unzip curl && \
  apk add --virtual build-dependencies \
    postgresql-dev \
    gcc \
    python3-dev \
    musl-dev && \
  pip install -r ${REQUIREMENTS_FILE} && \
  apk del build-dependencies && \
  rm -rf /var/cache/apk/*

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

COPY . /app

WORKDIR /app

CMD ["python", "load_dataset.py"]

ENV PATH /var/pydev/bin:$PATH
ENV PYTHONPATH /var/pydev
