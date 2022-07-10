from unittest import mock
from pathlib import Path
import psycopg2
import shutil

from .conftest import DATABASE_URL
from load_dataset import Config, load_dataset, TEST_DATA_DIR
import wowutil

MY_DIR = Path(__file__).parent.resolve()

OCA_TEST_DATA_DIR = MY_DIR / "data"


def copy_oca_test_data_to_nycdb_dir():
    for file in OCA_TEST_DATA_DIR.glob("*oca_*.csv"):
        shutil.copy(file, TEST_DATA_DIR)


def load_dependee_datasets(config: Config):
    for dataset_name in wowutil.WOW_YML["dependencies"]:
        load_dataset(dataset_name, config)


# TODO: ensure_oca_works()


def ensure_wow_works():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wow.wow_bldgs")
            assert cur.fetchone()[0] > 0

            # Make sure functions are defined, at least.
            cur.execute("SET search_path TO wow, public")
            cur.execute("SELECT wow.get_assoc_addrs_from_bbl('blah')")


def test_it_works(db, slack_outbox):

    # Let's intentionally disable our access to Algolio
    # so we don't update the landlord search index
    with mock.patch.dict("os.environ", {"ALGOLIA_API_KEY": ""}, clear=True):
        config = Config(database_url=DATABASE_URL, use_test_data=True)
        load_dependee_datasets(config)
        copy_oca_test_data_to_nycdb_dir()

        wowutil.main(["build", "--test=True"], db_url=DATABASE_URL)

        ensure_wow_works()

        assert "Rebuilding OCA evictions tables..." in slack_outbox
        assert "Rebuilding Who Owns What tables..." in slack_outbox
        assert "Rebuilding Algolia landlord index..." not in slack_outbox
        assert slack_outbox[-3] == "Finished rebuilding OCA evictions tables."
        assert slack_outbox[-1] == "Finished rebuilding Who Owns What tables."

        # Ensure that reloading the dependee datasets doesn't raise
        # an exception.
        load_dependee_datasets(config)

        # Ensure running build again doesn't raise an exception.
        wowutil.main(["build", "--test=True"], db_url=DATABASE_URL)

        ensure_wow_works()
