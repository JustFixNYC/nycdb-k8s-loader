import os
import time
import subprocess
from unittest.mock import patch
from typing import Dict
import urllib.parse
import pytest
import nycdb.datasets

from .conftest import DATABASE_URL, make_conn
import load_dataset
import show_rowcounts


@pytest.fixture()
def test_db_env(db):
    env = os.environ.copy()
    env['DATABASE_URL'] = DATABASE_URL
    env['USE_TEST_DATA'] = '1'
    env['DATASET'] = ''
    env['SLACK_WEBHOOK_URL'] = ''
    yield env


def drop_dataset_tables(conn, dataset: str, ok_if_nonexistent: bool):
    with conn.cursor() as cur:
        for table in load_dataset.get_tables_for_dataset(dataset):
            if_exists = 'IF EXISTS' if ok_if_nonexistent else ''
            cur.execute(f'DROP TABLE {if_exists} {table.name}')


def get_row_counts(conn, dataset: str) -> Dict[str, int]:
    tables = [table.name for table in load_dataset.get_tables_for_dataset(dataset)]
    return dict(show_rowcounts.get_rowcounts(conn, tables))


@pytest.mark.parametrize('dataset', nycdb.dataset.datasets().keys())
def test_load_dataset_works(test_db_env, dataset):
    with make_conn() as conn:
        drop_dataset_tables(conn, dataset, ok_if_nonexistent=True)

    subprocess.check_call([
        'python', 'load_dataset.py', dataset
    ], env=test_db_env)

    with make_conn() as conn:
        table_counts = get_row_counts(conn, dataset)

    assert len(table_counts) > 0
    for count in table_counts.values():
        assert count > 0

    # Ensure idempotency.

    subprocess.check_call([
        'python', 'load_dataset.py', dataset
    ], env=test_db_env)

    with make_conn() as conn:
        assert get_row_counts(conn, dataset) == table_counts
        drop_dataset_tables(conn, dataset, ok_if_nonexistent=False)


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


def test_get_temp_schemas_works(test_db_env, conn):
    with patch('time.time', return_value=1234.1446786):
        schema = load_dataset.create_temp_schema_name('boop')
        with load_dataset.create_and_enter_temporary_schema(conn, schema):
            assert load_dataset.get_temp_schemas(conn, 'boop') == [
                'temp_boop_1234'
            ]
        assert len(load_dataset.get_temp_schemas(conn, 'boop')) == 0
