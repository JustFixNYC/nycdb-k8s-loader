import os
import time
import subprocess
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pytest


import load_dataset


DATABASE_URL = f"{load_dataset.DATABASE_URL}_test"

CONNECT_ARGS = dict(
    user=load_dataset.DB_USER,
    password=load_dataset.DB_PASSWORD,
    host=load_dataset.DB_HOST,
    database=f"{load_dataset.DB_NAME}_test",
    port=load_dataset.DB_PORT
)


def create_db(dbname):
    args = CONNECT_ARGS.copy()
    del args['database']
    conn = psycopg2.connect(**args)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as curs:
        curs.execute('CREATE DATABASE ' + dbname)
    conn.close()


@pytest.fixture(scope="session")
def db():
    """
    Attempt to connect to the database, retrying if necessary, and also
    creating the database if it doesn't already exist.
    """

    retries_left = 5
    db = CONNECT_ARGS['database']

    while True:
        try:
            psycopg2.connect(**CONNECT_ARGS).close()
            return
        except psycopg2.OperationalError as e:
            if 'database "{}" does not exist'.format(db) in str(e):
                create_db(db)
                retries_left -= 1
            elif retries_left:
                # It's possible the database is still starting up.
                time.sleep(2)
                retries_left -= 1
            else:
                raise e


@pytest.fixture()
def test_db_env(db):
    env = os.environ.copy()
    env['DATABASE_URL'] = DATABASE_URL
    env['USE_TEST_DATA'] = '1'
    env['DATASET'] = ''
    env['SLACK_WEBHOOK_URL'] = ''
    yield env


def drop_table(name: str):
    with psycopg2.connect(**CONNECT_ARGS) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {name}')


def get_row_count(table_name: str):
    with psycopg2.connect(**CONNECT_ARGS) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {table_name}')
            return cur.fetchone()[0]


def test_load_dataset_works(test_db_env):
    drop_table('hpd_registrations')

    subprocess.check_call([
        'python', 'load_dataset.py', 'hpd_registrations'
    ], env=test_db_env)

    count = get_row_count('hpd_registrations')

    assert count > 0

    subprocess.check_call([
        'python', 'load_dataset.py', 'hpd_registrations'
    ], env=test_db_env)

    assert get_row_count('hpd_registrations') == count


def test_load_dataset_fails_if_no_dataset_provided(test_db_env):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call([
            'python', 'load_dataset.py'
        ], env=test_db_env)


def test_get_tables_for_dataset_raises_error_on_invalid_dataset():
    with pytest.raises(SystemExit):
        load_dataset.get_tables_for_dataset('blarg')
