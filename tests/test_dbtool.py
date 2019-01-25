from unittest.mock import patch
import subprocess
import pytest

import dbtool
from .conftest import DATABASE_URL


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
