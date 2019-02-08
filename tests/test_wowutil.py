from nycdb.dataset import Dataset

from .conftest import DATABASE_URL
from load_dataset import Config
import wowutil


def test_it_works(db, slack_outbox):
    config = Config(database_url=DATABASE_URL, use_test_data=True)
    for dataset_name in wowutil.WOW_NYCDB_DEPENDENCIES:
        ds = Dataset(dataset_name, args=config.nycdb_args)
        ds.db_import()
    wowutil.main(['build'], db_url=DATABASE_URL)
    assert slack_outbox[0] == 'Rebuilding Who Owns What tables...'
    assert slack_outbox[1] == 'Finished rebuilding Who Owns What tables.'
