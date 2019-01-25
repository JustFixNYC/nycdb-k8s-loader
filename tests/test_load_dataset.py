import os
import time
import subprocess
from unittest.mock import patch
from typing import Dict
import urllib.parse
import pytest
import nycdb.dataset

from .conftest import DATABASE_URL, make_conn
import load_dataset
import dbtool


@pytest.fixture()
def test_db_env(db):
    env = os.environ.copy()
    env['DATABASE_URL'] = DATABASE_URL
    env['USE_TEST_DATA'] = '1'
    env['DATASET'] = ''
    env['SLACK_WEBHOOK_URL'] = ''
    yield env


def test_get_dataset_tables_included_derived_tables():
    info = load_dataset.TableInfo(
        name='hpd_registrations_grouped_by_bbl',
        dataset='hpd_registrations'
    )
    assert info in load_dataset.get_dataset_tables()


def drop_dataset_tables(conn, dataset: str, ok_if_nonexistent: bool):
    with conn.cursor() as cur:
        for table in load_dataset.get_tables_for_dataset(dataset):
            if_exists = 'IF EXISTS' if ok_if_nonexistent else ''
            cur.execute(f'DROP TABLE {if_exists} {table.name}')


def get_row_counts(conn, dataset: str) -> Dict[str, int]:
    tables = [table.name for table in load_dataset.get_tables_for_dataset(dataset)]
    return dict(dbtool.get_rowcounts(conn, tables))


def test_get_urls_for_dataset_works():
    urls = load_dataset.get_urls_for_dataset('hpd_registrations')
    assert len(urls) > 0
    for url in urls:
        assert url.startswith('https://')


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


def test_exceptions_send_slack_msg(slack_outbox):
    with patch.object(load_dataset, 'load_dataset') as load:
        load.side_effect = Exception('blah')
        with pytest.raises(Exception, match='blah'):
            load_dataset.main(['', 'hpd_registrations'])
        load.assert_called_once_with('hpd_registrations')
        assert slack_outbox == [
            'Alas, an error occurred when loading the dataset `hpd_registrations`.'
        ]


def test_unmodified_datasets_are_not_retrieved(db, requests_mock, slack_outbox):
    urls = load_dataset.get_urls_for_dataset('hpd_registrations')
    config = load_dataset.Config(database_url=DATABASE_URL, use_test_data=True)
    load = lambda: load_dataset.load_dataset('hpd_registrations', config, force_check_urls=True)

    for url in urls:
        requests_mock.get(url, text='blah', headers={'ETag': 'blah'})

    load()
    assert slack_outbox[0] == 'Downloading the dataset `hpd_registrations`...'
    slack_outbox[:] = []

    for url in urls:
        requests_mock.get(url, request_headers={'If-None-Match': 'blah'}, status_code=304)
    load()
    assert slack_outbox == [
        'The dataset `hpd_registrations` has not changed since we last retrieved it.'
    ]
    slack_outbox[:] = []

    for url in urls:
        requests_mock.get(url, text='blah2', headers={'ETag': 'blah2'})
    load()
    assert slack_outbox[0] == 'Downloading the dataset `hpd_registrations`...'
