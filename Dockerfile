FROM python:3.6

COPY requirements* /

ARG REQUIREMENTS_FILE=requirements.txt

RUN apt-get update && \
  apt-get install -y \
    unzip \
    postgresql-client && \
  rm -rf /var/lib/apt/lists/*

RUN pip install -r ${REQUIREMENTS_FILE}

ARG NYCDB_REPO=https://github.com/toolness/nyc-db
ARG NYCDB_REV=c151e2366b20050eb54d562105a3c63f5699bf1d

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
ARG WOW_REV=1609c51f1ddada6accf13049691d88e2e7755a07
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
