"""\
Create Good Cause Eviction eligibility data tables on the Who Owns What database.

Usage:
  goodcauseutil.py build [options]

Options:
  -h --help     Show this screen.

Environment variables:
  DATABASE_URL           The URL of the NYC-DB and WoW database.
"""

import os
import sys
from typing import List

import docopt
import psycopg

from lib import slack
from lib.dataset_tracker import DatasetTracker
from load_dataset import (
    create_temp_schema_name,
    create_and_enter_temporary_schema,
    get_dataset_dbhash,
    save_and_reapply_permissions,
    ensure_schema_exists,
    drop_tables_if_they_exist,
    change_table_schemas,
    TableInfo,
)
from wowutil import WOW_SQL_DIR, WOW_YML, install_db_extensions

WOW_SCHEMA = "wow"

GOOD_CAUSE_TABLES: List[str] = ["gce_screener"]


def create_and_populate_good_cause_tables(conn, is_testing: bool = False):
    import goodcause.table

    config = goodcause.table.GoodCauseConfig(
        sql_dir=WOW_SQL_DIR,
        sql_files=WOW_YML["good_cause_sql"],
    )

    with conn.cursor() as wow_cur:
        goodcause.table.populate_tables(wow_cur, config)

    conn.commit()


def build(db_url: str):
    slack.sendmsg("Rebuilding Good Cause Eviction tables...")

    cosmetic_dataset_name = "good_cause_eviction"

    tables = [
        TableInfo(name=name, dataset=cosmetic_dataset_name)
        for name in GOOD_CAUSE_TABLES
    ]

    with psycopg.connect(db_url) as conn:
        install_db_extensions(conn)
        dataset_dbhash = get_dataset_dbhash(conn)
        dataset_tracker = DatasetTracker(cosmetic_dataset_name, dataset_dbhash)
        temp_schema = create_temp_schema_name(cosmetic_dataset_name)
        with create_and_enter_temporary_schema(conn, temp_schema):
            create_and_populate_good_cause_tables(conn)
            ensure_schema_exists(conn, WOW_SCHEMA)
            with save_and_reapply_permissions(conn, tables, WOW_SCHEMA):
                drop_tables_if_they_exist(conn, tables, WOW_SCHEMA)
                change_table_schemas(conn, tables, temp_schema, WOW_SCHEMA)

        # Note that if we ever add SQL functions to the GCE dataset we'll
        # need to implement the same pattern as in wowutil to recreate them in
        # the final WOW schema.

    dataset_tracker.update_tracker()
    slack.sendmsg("Finished rebuilding Good Cause Eviction tables.")


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    if args["build"]:
        build(db_url)


if __name__ == "__main__":
    main(argv=sys.argv[1:], db_url=os.environ["DATABASE_URL"])
