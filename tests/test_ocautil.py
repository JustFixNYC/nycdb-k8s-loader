from pathlib import Path
import psycopg2
import shutil

from .conftest import DATABASE_URL
from load_dataset import Config, TEST_DATA_DIR
import ocautil

MY_DIR = Path(__file__).parent.resolve()

OCA_TEST_DATA_DIR = MY_DIR / "data"


def copy_oca_test_data_to_nycdb_dir():
    for file in OCA_TEST_DATA_DIR.glob("*oca_*.csv"):
        shutil.copy(file, TEST_DATA_DIR)


def ensure_oca_works():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wow.oca_evictions_bldgs")
            assert cur.fetchone()[0] > 0


def test_it_works(db, slack_outbox):

    copy_oca_test_data_to_nycdb_dir()

    ocautil.main(["build", "--test=True"], db_url=DATABASE_URL)

    ensure_oca_works()

    assert "Rebuilding OCA evictions tables..." in slack_outbox
    assert slack_outbox[-1] == "Finished rebuilding OCA evictions tables."

    # Ensure running build again doesn't raise an exception.
    ocautil.main(["build", "--test=True"], db_url=DATABASE_URL)

    ensure_oca_works()
