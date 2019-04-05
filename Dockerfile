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
ARG NYCDB_REV=cea1ca97adc21bc8a84791b0ccc01ba551e0f6b2

# We need to retrieve the source directly from the repository
# because we need access to the test data, which isn't part of
# the pypi distribution.
RUN curl -L ${NYCDB_REPO}/archive/${NYCDB_REV}.zip > nyc-db.zip \
  && unzip nyc-db.zip \
  && rm nyc-db.zip \
  && mv nyc-db-${NYCDB_REV} nyc-db \
  && cd nyc-db/src \
  && pip install -e .

ARG WOW_REPO=https://github.com/justFixNYC/who-owns-what
ARG WOW_REV=cae0b6de35eba25df3376f6e890312aa55356c98
RUN curl -L ${WOW_REPO}/archive/${WOW_REV}.zip > wow.zip \
  && unzip wow.zip \
  && rm wow.zip \
  && mv who-owns-what-${WOW_REV} who-owns-what

COPY . /app

WORKDIR /app

CMD ["python", "load_dataset.py"]

ENV PATH /var/pydev/bin:$PATH
ENV PYTHONPATH /var/pydev
ENV PYTHONUNBUFFERED yup
