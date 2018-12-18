import os
import time
import subprocess
from unittest.mock import patch
from typing import Dict
import psycopg2
import urllib.parse
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pytest
import nycdb.datasets

import load_dataset
import show_rowcounts


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


def drop_dataset_tables(dataset: str, ok_if_nonexistent: bool):
    with psycopg2.connect(**CONNECT_ARGS) as conn:
        with conn.cursor() as cur:
            for table in load_dataset.get_tables_for_dataset(dataset):
                if_exists = 'IF EXISTS' if ok_if_nonexistent else ''
                cur.execute(f'DROP TABLE {if_exists} {table.name}')


def get_row_counts(dataset: str) -> Dict[str, int]:
    tables = [table.name for table in load_dataset.get_tables_for_dataset(dataset)]
    with psycopg2.connect(DATABASE_URL) as conn:
        return dict(show_rowcounts.get_rowcounts(conn, tables))


@pytest.mark.parametrize('dataset', nycdb.dataset.datasets().keys())
def test_load_dataset_works(test_db_env, dataset):
    drop_dataset_tables(dataset, ok_if_nonexistent=True)

    subprocess.check_call([
        'python', 'load_dataset.py', dataset
    ], env=test_db_env)

    table_counts = get_row_counts(dataset)
    assert len(table_counts) > 0
    for count in table_counts.values():
        assert count > 0

    # Ensure idempotency.

    subprocess.check_call([
        'python', 'load_dataset.py', dataset
    ], env=test_db_env)

    assert get_row_counts(dataset) == table_counts
    drop_dataset_tables(dataset, ok_if_nonexistent=False)


def test_load_dataset_fails_if_no_dataset_provided(test_db_env):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call([
            'python', 'load_dataset.py'
        ], env=test_db_env)


def test_get_tables_for_dataset_raises_error_on_invalid_dataset():
    with pytest.raises(SystemExit):
        load_dataset.get_tables_for_dataset('blarg')


def test_temp_schema_naming_works():
    with patch('time.time', return_value=1545146760.1446786):
        name = load_dataset.create_temp_schema_name('boop')
        assert name == 'temp_boop_1545146760'
        ts = load_dataset.get_friendly_temp_schema_creation_time(name)
        assert ts == '2018-12-18 15:26:00 UTC'


def test_get_temp_schemas_works(test_db_env):
    with psycopg2.connect(DATABASE_URL) as conn:
        with patch('time.time', return_value=1234.1446786):
            schema = load_dataset.create_temp_schema_name('boop')
            with load_dataset.create_and_enter_temporary_schema(conn, schema):
                assert load_dataset.get_temp_schemas(conn, 'boop') == [
                    'temp_boop_1234'
                ]
            assert len(load_dataset.get_temp_schemas(conn, 'boop')) == 0
