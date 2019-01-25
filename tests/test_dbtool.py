import pytest

import dbtool


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
