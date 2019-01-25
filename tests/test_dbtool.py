from unittest.mock import patch
import contextlib
import subprocess
import psycopg2
import pytest

import dbtool
import load_dataset
from lib.lastmod import LastmodInfo
from .conftest import DATABASE_URL


HPD_REG_URL = load_dataset.get_urls_for_dataset('hpd_registrations')[0]


def test_get_tables_for_datasets_works():
    assert len(dbtool.get_tables_for_datasets(['acris'])) > 1


def test_validate_and_get_dataset_names_works():
    assert len(dbtool.validate_and_get_dataset_names(['all'])) > 1
    assert dbtool.validate_and_get_dataset_names(['oath_hearings']) == [
        'oath_hearings'
    ]


def test_validate_and_get_dataset_names_raises_error():
    with pytest.raises(SystemExit):
        dbtool.validate_and_get_dataset_names(['oath_hearings', 'boop'])


def test_shell_works():
    with patch('nycdb.cli.run_dbshell') as m:
        dbtool.main(['shell'], DATABASE_URL)
    m.assert_called_once()


def test_rowcounts_works(test_db_env, capsys):
    subprocess.check_call([
        'python', 'load_dataset.py', 'hpd_registrations'
    ], env=test_db_env)
    capsys.readouterr()

    dbtool.main(['rowcounts', 'hpd_registrations'], DATABASE_URL)
    out, err = capsys.readouterr()

    assert "hpd_registrations has 100 rows" in out
    assert "hpd_contacts has 100 rows" in out


@contextlib.contextmanager
def load_dbhash():
    with psycopg2.connect(DATABASE_URL) as conn:
        dbhash = load_dataset.get_dbhash(conn)
        yield dbhash


def test_list_lastmod_works(db, capsys):
    def list_lastmod():
        dbtool.main(['lastmod:list', 'hpd_registrations'], DATABASE_URL)
        out, err = capsys.readouterr()
        return out

    assert "has no metadata about its last modification" in list_lastmod()

    url = HPD_REG_URL
    with load_dbhash() as dbhash:
        info = LastmodInfo(url, etag='blah', last_modified='Tue, 01 Jan 2019 20:56:28 UTC')
        info.write_to_dbhash(dbhash)

    assert "was last modified on Tue, 01 Jan 2019 20:56:28 UTC" in list_lastmod()


def test_reset_lastmod_works(db, capsys):
    url = HPD_REG_URL
    with load_dbhash() as dbhash:
        info = LastmodInfo(url, etag='blah', last_modified='Tue, 01 Jan 2019 20:56:28 UTC')
        info.write_to_dbhash(dbhash)

    dbtool.main(['lastmod:reset', 'hpd_registrations'], DATABASE_URL)

    with load_dbhash() as dbhash:
        info = LastmodInfo.read_from_dbhash(url, dbhash)
        assert info == LastmodInfo(url=url, etag=None, last_modified=None)
