import psycopg2

from .conftest import DATABASE_URL
from load_dataset import Config, load_dataset
import wowutil


def load_dependee_datasets(config: Config):
    for dataset_name in wowutil.WOW_YML['dependencies']:
        load_dataset(dataset_name, config)


def ensure_wow_works():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wow.wow_bldgs")
            assert cur.fetchone()[0] > 0

            # Make sure functions are defined, at least.
            cur.execute("SET search_path TO wow, public")
            cur.execute("SELECT wow.get_assoc_addrs_from_bbl('blah')")


def test_it_works(db, slack_outbox):
    config = Config(database_url=DATABASE_URL, use_test_data=True)
    load_dependee_datasets(config)
    wowutil.main(['build'], db_url=DATABASE_URL)

    ensure_wow_works()
    assert slack_outbox[-2] == 'Rebuilding Who Owns What tables...'
    assert slack_outbox[-1] == 'Finished rebuilding Who Owns What tables.'

    # Ensure that reloading the dependee datasets doesn't raise
    # an exception.
    load_dependee_datasets(config)

    # Ensure runing build again doesn't raise an exception.
    wowutil.main(['build'], db_url=DATABASE_URL)

    ensure_wow_works()
