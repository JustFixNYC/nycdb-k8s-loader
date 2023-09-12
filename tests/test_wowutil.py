from unittest import mock
import psycopg2

from .conftest import DATABASE_URL
from load_dataset import Config, load_dataset, NYCDB_DATA_DIR, TEST_DATA_DIR
import wowutil


def load_dependee_datasets(config: Config):
    for dataset_name in wowutil.WOW_YML["dependencies"]:
        load_dataset(dataset_name, config)


def create_empty_oca_tables():
    import ocaevictions.table

    oca_config = ocaevictions.table.OcaConfig(
        sql_dir=wowutil.WOW_SQL_DIR,
        data_dir=NYCDB_DATA_DIR,
        test_dir=TEST_DATA_DIR,
        aws_key=None,
        aws_secret=None,
        s3_bucket=None,
        sql_pre_files=wowutil.WOW_YML["oca_pre_sql"],
        sql_post_files=wowutil.WOW_YML["oca_post_sql"],
        s3_objects=wowutil.WOW_YML["oca_s3_objects"],
        is_testing=True,
    )

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            ocaevictions.table.create_oca_s3_tables(cur, oca_config)
            ocaevictions.table.create_derived_oca_tables(cur, oca_config)
            


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
        create_empty_oca_tables()

        wowutil.main(["build"], db_url=DATABASE_URL)

        ensure_wow_works()

        assert "Rebuilding Who Owns What tables..." in slack_outbox
        assert "Rebuilding Algolia landlord index..." not in slack_outbox
        assert slack_outbox[-1] == "Finished rebuilding Who Owns What tables."

        # Ensure that reloading the dependee datasets doesn't raise
        # an exception.
        load_dependee_datasets(config)

        # Ensure running build again doesn't raise an exception.
        wowutil.main(["build"], db_url=DATABASE_URL)

        ensure_wow_works()
