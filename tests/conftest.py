import os
import time
import urllib.parse
from unittest.mock import patch
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pytest

DATABASE_URL = os.environ['TEST_DATABASE_URL']

DB_INFO = urllib.parse.urlparse(DATABASE_URL)
DB_PORT = DB_INFO.port or 5432
DB_HOST = DB_INFO.hostname
DB_NAME = DB_INFO.path[1:]
DB_USER = DB_INFO.username
DB_PASSWORD = DB_INFO.password

CONNECT_ARGS = dict(
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    database=DB_NAME,
    port=DB_PORT
)


def exec_outside_of_transaction(sql: str):
    args = CONNECT_ARGS.copy()
    del args['database']
    conn = psycopg2.connect(**args)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as curs:
        curs.execute(sql)
    conn.close()


def drop_db(dbname):
    exec_outside_of_transaction('DROP DATABASE ' + dbname)


def create_db(dbname):
    exec_outside_of_transaction('CREATE DATABASE ' + dbname)


@pytest.fixture(scope="session")
def db():
    """
    Attempt to connect to the database, retrying if necessary, and also
    creating the database if it doesn't already exist.
    """

    retries_left = 5
    db = CONNECT_ARGS['database']
    created = False

    while True:
        try:
            psycopg2.connect(**CONNECT_ARGS).close()
            break
        except psycopg2.OperationalError as e:
            if 'database "{}" does not exist'.format(db) in str(e):
                create_db(db)
                created = True
                retries_left -= 1
            elif retries_left:
                # It's possible the database is still starting up.
                time.sleep(2)
                retries_left -= 1
            else:
                raise e

    if not created:
        drop_db(db)
        create_db(db)


@pytest.fixture()
def conn(db):
    with make_conn() as conn:
        yield conn


def make_conn():
    return psycopg2.connect(DATABASE_URL)


@pytest.fixture()
def slack_outbox():
    slack_outbox = []
    log_slack_msg = lambda x: slack_outbox.append(x)

    with patch('slack.sendmsg', side_effect=log_slack_msg):
        yield slack_outbox
