"""\
Create OCA evictions data tables on the Who Owns What database.

Usage:
  ocautil.py build [options] [test]

Options:
  -h --help     Show this screen.

Test:
  --test=<bool>          Use test data if the argument==True    

Environment variables:
  DATABASE_URL           The URL of the NYC-DB and WoW database.
"""

import os
import sys
from pathlib import Path
from typing import List

import docopt
import psycopg2
import yaml

from lib import slack
from load_dataset import (
    create_temp_schema_name,
    create_and_enter_temporary_schema,
    save_and_reapply_permissions,
    ensure_schema_exists,
    drop_tables_if_they_exist,
    change_table_schemas,
    TableInfo,
    NYCDB_DATA_DIR,
    TEST_DATA_DIR,
)
from wowutil import install_db_extensions

WOW_SCHEMA = "wow"

WOW_DIR = Path("/who-owns-what")

WOW_SQL_DIR = Path(WOW_DIR / "sql")

WOW_YML = yaml.load((WOW_DIR / "who-owns-what.yml").read_text(), Loader=yaml.FullLoader)

OCA_TABLES: List[str] = WOW_YML["oca_tables"]


def create_and_populate_oca_tables(conn, is_testing: bool = False):
    import ocaevictions.table

    config = ocaevictions.table.OcaConfig(
        oca_table_names=OCA_TABLES,
        sql_dir=WOW_SQL_DIR,
        data_dir=NYCDB_DATA_DIR,
        test_dir=TEST_DATA_DIR,
        oca_db_url=os.environ.get("OCA_DATABASE_URL", None),
        oca_ssh_host=os.environ.get("OCA_SSH_HOST", None),
        oca_ssh_user=os.environ.get("OCA_SSH_USER", None),
        oca_ssh_pkey=os.environ.get("OCA_SSH_PKEY", None),
        is_testing=is_testing,
    )

    with conn.cursor() as wow_cur:
        ocaevictions.table.create_oca_tables(wow_cur, config)
        ocaevictions.table.populate_oca_tables(wow_cur, config)

    conn.commit()


def build(db_url: str, is_testing: bool = False):
    slack.sendmsg("Rebuilding OCA evictions tables...")

    cosmetic_dataset_name = "oca_address"

    tables = [
        TableInfo(name=name, dataset=cosmetic_dataset_name) for name in OCA_TABLES
    ]

    with psycopg2.connect(db_url) as conn:
        install_db_extensions(conn)
        temp_schema = create_temp_schema_name(cosmetic_dataset_name)
        with create_and_enter_temporary_schema(conn, temp_schema):
            create_and_populate_oca_tables(conn, is_testing)
            ensure_schema_exists(conn, WOW_SCHEMA)
            with save_and_reapply_permissions(conn, tables, WOW_SCHEMA):
                drop_tables_if_they_exist(conn, tables, WOW_SCHEMA)
                change_table_schemas(conn, tables, temp_schema, WOW_SCHEMA)

        # Note that if we ever add SQL functions to the OCA dataset we'll need
        # to implement the same pattern as in wowutil to recreate them in the
        # final WOW schema.

    slack.sendmsg("Finished rebuilding OCA evictions tables.")


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    if args["build"]:
        is_testing = bool(args["--test"])
        build(db_url, is_testing)


if __name__ == "__main__":
    main(argv=sys.argv[1:], db_url=os.environ["DATABASE_URL"])
