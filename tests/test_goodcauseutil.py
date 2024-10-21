from unittest import mock
from pathlib import Path
import psycopg2
import subprocess

from tests.test_wowutil import create_empty_oca_tables, load_dependee_datasets
from .conftest import DATABASE_URL
from load_dataset import Config
import wowutil
import goodcauseutil

MY_DIR = Path(__file__).parent.resolve()


def ensure_goodcause_works():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wow.gce_eligibility")
            r = cur.fetchone()
            assert r[0] > 0


def test_it_works(test_db_env, slack_outbox):
    # Let's intentionally disable our access to Algolio
    # so we don't update the landlord search index
    with mock.patch.dict("os.environ", {"ALGOLIA_API_KEY": ""}, clear=True):
        # load wow depenedencies (since wow itself is GCE dependency)
        config = Config(database_url=DATABASE_URL, use_test_data=True)
        load_dependee_datasets(config)
        create_empty_oca_tables()

        # need these additional tables to build good cause eviction data
        dependency_datasets = [
            "fc_shd",
            "nycha_bbls",
            "hpd_ll44",
            "dob_certificate_occupancy",
        ]
        for dataset in dependency_datasets:
            subprocess.check_call(["python", "load_dataset.py", dataset], env=test_db_env)

        wowutil.main(["build"], db_url=DATABASE_URL)

        goodcauseutil.main(["build", "--test=True"], db_url=DATABASE_URL)

        ensure_goodcause_works()

        assert "Rebuilding Good Cause Eviction tables..." in slack_outbox
        assert slack_outbox[-1] == "Finished rebuilding Good Cause Eviction tables."

        # Ensure running build again doesn't raise an exception.
        goodcauseutil.main(["build", "--test=True"], db_url=DATABASE_URL)

        ensure_goodcause_works()
