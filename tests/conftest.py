import os
import time
import urllib.parse
from unittest.mock import patch
import psycopg
import pytest

DATABASE_URL = os.environ["TEST_DATABASE_URL"]

DB_INFO = urllib.parse.urlparse(DATABASE_URL)
DB_PORT = DB_INFO.port or 5432
DB_HOST = DB_INFO.hostname
DB_NAME = DB_INFO.path[1:]
DB_USER = DB_INFO.username
DB_PASSWORD = DB_INFO.password

CONNECT_ARGS = dict(
    user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME, port=DB_PORT
)


def exec_outside_of_transaction(sql: str):
    args = CONNECT_ARGS.copy()
    del args["database"]
    conn = psycopg.connect(**args)
    conn.isolation_level = psycopg.IsolationLevel.READ_UNCOMMITTED
    with conn.cursor() as curs:
        curs.execute(sql)
    conn.close()


def drop_db(dbname):
    exec_outside_of_transaction("DROP DATABASE " + dbname)


def create_db(dbname):
    exec_outside_of_transaction("CREATE DATABASE " + dbname)


@pytest.fixture()
def db():
    """
    Attempt to connect to the database, retrying if necessary, and also
    creating the database if it doesn't already exist.

    This will also ensure the database is empty when the test starts.
    """

    retries_left = 5
    db = CONNECT_ARGS["database"]
    created = False

    while True:
        try:
            psycopg.connect(**CONNECT_ARGS).close()
            created = True
            break
        except psycopg.OperationalError as e:
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
    return psycopg.connect(DATABASE_URL)


@pytest.fixture()
def slack_outbox():
    slack_outbox = []
    log_slack_msg = lambda x, **kwargs: slack_outbox.append(x)

    with patch("lib.slack.sendmsg", side_effect=log_slack_msg):
        yield slack_outbox


@pytest.fixture()
def test_db_env(db):
    env = os.environ.copy()
    env["DATABASE_URL"] = DATABASE_URL
    env["USE_TEST_DATA"] = "1"
    env["DATASET"] = ""
    env["SLACK_WEBHOOK_URL"] = ""
    yield env
