from pathlib import Path
import psycopg2
import shutil
import subprocess

from .conftest import DATABASE_URL
from load_dataset import TEST_DATA_DIR
import signatureutil

MY_DIR = Path(__file__).parent.resolve()

SIGNATURE_TEST_DATA_DIR = MY_DIR / "data"


def copy_signature_test_data_to_nycdb_dir():
    for file in SIGNATURE_TEST_DATA_DIR.glob("*signature_*.csv"):
        shutil.copy(file, TEST_DATA_DIR)


def ensure_signature_works():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM signature.signature_buildings")
            r = cur.fetchone()
            assert r[0] > 0


def test_it_works(test_db_env, slack_outbox):

    # need these tables to build signature
    dependency_datasets = [
        "pluto_latest",
        "boundaries",
        "hpd_violations",
        "hpd_complaints",
        "marshal_evictions",
        "oca",
        "oca_address",
        "hpd_litigations",
        "rentstab_v2",
        "hpd_charges",
        "hpd_aep",
        "hpd_underlying_conditions",
        "hpd_conh",
        "hpd_registrations",
        "acris",
        "dohmh_rodent_inspections",
        "dobjobs",
        "dob_violations",
        "ecb_violations",
    ]
    for dataset in dependency_datasets:
        subprocess.check_call(["python", "load_dataset.py", dataset], env=test_db_env)

    copy_signature_test_data_to_nycdb_dir()

    signatureutil.main(["build", "--test=True"], db_url=DATABASE_URL)

    ensure_signature_works()

    assert "Rebuilding Signature tables..." in slack_outbox
    assert slack_outbox[-1] == "Finished rebuilding Signature tables."

    # Ensure running build again doesn't raise an exception.
    signatureutil.main(["build", "--test=True"], db_url=DATABASE_URL)

    ensure_signature_works()
