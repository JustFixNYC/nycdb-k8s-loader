import psycopg2
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

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wow.wow_bldgs")
            assert cur.fetchone()[0] > 0

            # Make sure functions are defined, at least.
            cur.execute("SET search_path TO wow, public")
            cur.execute("SELECT wow.get_assoc_addrs_from_bbl('blah')")

    assert slack_outbox[0] == 'Rebuilding Who Owns What tables...'
    assert slack_outbox[1] == 'Finished rebuilding Who Owns What tables.'
