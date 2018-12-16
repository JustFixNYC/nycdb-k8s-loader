import pytest

import show_rowcounts as sr


def test_get_tables_for_datasets_works():
    assert len(sr.get_tables_for_datasets(['acris'])) > 1


def test_validate_and_get_dataset_names_works():
    assert len(sr.validate_and_get_dataset_names(['all'])) > 1
    assert sr.validate_and_get_dataset_names(['oath_hearings']) == [
        'oath_hearings'
    ]


def test_validate_and_get_dataset_names_raises_error():
    with pytest.raises(SystemExit):
        sr.validate_and_get_dataset_names(['oath_hearings', 'boop'])
