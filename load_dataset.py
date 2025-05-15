#!/usr/bin/env python

import os
import sys
import contextlib
import time
import re
from pathlib import Path
from typing import NamedTuple, List
from types import SimpleNamespace
from contextlib import contextmanager
import urllib.parse
from lib.dataset_tracker import DatasetTracker
import rollbar
import nycdb
import nycdb.dataset
from nycdb.dataset import Dataset
from nycdb.utility import list_wrap

from lib import slack, db_perms
from lib.parse_created_tables import parse_nycdb_created_tables
from lib.lastmod import UrlModTracker
from lib.dbhash import SqlDbHash


MY_DIR = Path(__file__).parent.resolve()
NYCDB_DIR = Path("/nycdb/src")
TEST_DATA_DIR = NYCDB_DIR / "tests" / "integration" / "data"
NYCDB_DATA_DIR = Path("/var/nycdb")

ROLLBAR_ACCESS_TOKEN = os.environ.get("ROLLBAR_ACCESS_TOKEN", "")


class CommandError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class Config(NamedTuple):
    database_url: str = os.environ["DATABASE_URL"]
    use_test_data: bool = bool(os.environ.get("USE_TEST_DATA", ""))

    @property
    def nycdb_args(self):
        DB_INFO = urllib.parse.urlparse(self.database_url)
        DB_PORT = DB_INFO.port or 5432
        DB_HOST = DB_INFO.hostname
        DB_NAME = DB_INFO.path[1:]
        DB_USER = DB_INFO.username
        DB_PASSWORD = DB_INFO.password

        return SimpleNamespace(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME,
            port=str(DB_PORT),
            root_dir=str(TEST_DATA_DIR) if self.use_test_data else str(NYCDB_DATA_DIR),
            hide_progress=False,
        )


class TableInfo(NamedTuple):
    name: str
    dataset: str


def create_temp_schema_name(dataset: str) -> str:
    return f"{get_temp_schema_prefix(dataset)}{int(time.time())}"


def get_temp_schema_prefix(dataset: str) -> str:
    return f"temp_{dataset}_"


def get_friendly_temp_schema_creation_time(name: str) -> str:
    t = time.gmtime(int(name.split("_")[-1]))
    return time.strftime("%Y-%m-%d %H:%M:%S", t) + " UTC"


def get_temp_schemas(conn, dataset: str) -> List[str]:
    prefix = get_temp_schema_prefix(dataset)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT schema_name from information_schema.schemata "
            f"WHERE schema_name LIKE '{prefix}%'"
        )
        return [row[0] for row in cur.fetchall()]


def get_dataset_tables() -> List[TableInfo]:
    result: List[TableInfo] = []
    for dataset_name, info in nycdb.dataset.datasets().items():
        schemas = list_wrap(info["schema"]) if "schema" in info else []
        for schema in schemas:
            result.append(TableInfo(name=schema["table_name"], dataset=dataset_name))
        result.extend(
            [
                TableInfo(name=name, dataset=dataset_name)
                for name in parse_nycdb_created_tables(info.get("sql", []))
            ]
        )
    return result


def get_tables_for_dataset(dataset: str) -> List[TableInfo]:
    tables = [table for table in get_dataset_tables() if table.dataset == dataset]
    if not tables:
        raise CommandError(f"'{dataset}' is not a valid dataset.")
    return tables


def get_urls_for_dataset(dataset: str) -> List[str]:
    if "files" not in nycdb.dataset.datasets()[dataset]:
        return []
    return [fileinfo["url"] for fileinfo in nycdb.dataset.datasets()[dataset]["files"]]


def get_all_create_function_sql(root_dir: Path, sql_files: List[str]) -> str:
    """
    Given the SQL files in the given root directory, concatenate the
    contents of only the ones that contain "CREATE OR REPLACE FUNCTION"
    SQL statements. It's assumed that these particular SQL files are
    idempotent.
    """

    sqls: List[str] = []

    for sql_file in sql_files:
        sql = (root_dir / sql_file).read_text()
        if does_sql_create_functions(sql):
            sqls.append(sql)

    return "\n".join(sqls)


def get_all_create_function_sql_for_dataset(dataset: str) -> str:
    return get_all_create_function_sql(
        root_dir=Path(nycdb.__file__).parent.resolve() / "sql",
        sql_files=nycdb.dataset.datasets()[dataset].get("sql", []),
    )


def run_sql_if_nonempty(conn, sql: str, initial_sql: str = ""):
    if sql:
        with conn.cursor() as cur:
            if initial_sql:
                cur.execute(initial_sql)
            cur.execute(sql)
        conn.commit()


def collapse_whitespace(text: str) -> str:
    return re.sub(r"\W+", " ", text)


def does_sql_create_functions(sql: str) -> bool:
    return "CREATE OR REPLACE FUNCTION" in collapse_whitespace(sql).upper()


def drop_tables_if_they_exist(conn, tables: List[TableInfo], schema: str):
    with conn.cursor() as cur:
        for table in tables:
            name = f"{schema}.{table.name}"
            print(f"Dropping table '{name}' if it exists.")
            cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
    conn.commit()


@contextlib.contextmanager
def save_and_reapply_permissions(conn, tables: List[TableInfo], schema: str):
    """
    Holy hell this is annoying. See this issue for details:
    https://github.com/JustFixNYC/nycdb-k8s-loader/issues/5
    """

    # Create blank placeholder tables if they don't already exist,
    # so that any users with default privileges in our schema
    # have expected permissions on them.
    create_blank_tables = ";".join(
        [f"CREATE TABLE IF NOT EXISTS {schema}.{table.name} ()" for table in tables]
    )
    with conn.cursor() as cur:
        cur.execute(create_blank_tables)
    conn.commit()

    # Now remember the permissions on the tables.
    grants = "".join(
        db_perms.get_grant_sql(conn, table.name, schema) for table in tables
    )

    # Let the code inside our "with" clause run. It will likely
    # drop the tables and replace them with new ones that have
    # the same name.
    yield

    # Now grant the same permissions to the new tables.
    db_perms.exec_grant_sql(conn, grants)


def ensure_schema_exists(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.commit()


@contextlib.contextmanager
def create_and_enter_temporary_schema(conn, schema: str):
    print(f"Creating and entering temporary schema '{schema}'.")
    with conn.cursor() as cur:
        cur.execute(
            "; ".join(
                [
                    f"DROP SCHEMA IF EXISTS {schema} CASCADE",
                    f"CREATE SCHEMA {schema}",
                    # Note that we still need public at the end of the search
                    # path since we want functions like first(), which are
                    # declared in the public schema, to work. And we need oca
                    # because those tables are used for wow_bldgs.
                    f"SET search_path TO {schema}, public, oca, wow",
                ]
            )
        )
    conn.commit()

    try:
        yield
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        print(f"Destroying temporary schema '{schema}'.")
        with conn.cursor() as cur:
            cur.execute(
                "; ".join(
                    [f"DROP SCHEMA {schema} CASCADE", f"SET search_path TO public"]
                )
            )
        conn.commit()


def change_table_schemas(
    conn, tables: List[TableInfo], from_schema: str, to_schema: str
):
    with conn.cursor() as cur:
        for table in tables:
            name = f"{from_schema}.{table.name}"
            print(f"Setting table '{name}' schema to '{to_schema}'.")
            cur.execute(f"ALTER TABLE {name} SET SCHEMA {to_schema}")
    conn.commit()


def sanity_check():
    assert TEST_DATA_DIR.exists()


def get_url_dbhash(conn) -> SqlDbHash:
    ensure_schema_exists(conn, "nycdb_k8s_loader")
    return SqlDbHash(conn, "nycdb_k8s_loader.dbhash")


def get_dataset_dbhash(conn) -> SqlDbHash:
    ensure_schema_exists(conn, "nycdb_k8s_loader")
    return SqlDbHash(conn, "nycdb_k8s_loader.dataset_tracker")


def reset_files_if_test(dataset: Dataset, config: Config = Config()) -> Dataset:
    """
    Some nycdb datasets have a very large number of individual files, and only a
    few are included in the test data, so if load_dataset is being run as part of
    the test suite we need to edit the Dataset so that the files match those in the
    test data. Otherwise, all of the files not included in the test data will be
    downloaded in full and loaded into the DB as part of the tests.
    """
    if not config.use_test_data:
        return dataset

    if dataset.name == "dof_annual_sales":
        dataset.files = [
            nycdb.file.File(
                {
                    "dest": "dof_annual_sales_2020_manhattan.xlsx",
                    "url": "https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/2020/2020_manhattan.xlsx",
                },
                root_dir=str(TEST_DATA_DIR),
            ),
            nycdb.file.File(
                {
                    "dest": "dof_annual_sales_2015_manhattan.xls",
                    "url": "https://www1.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/2015/2015_manhattan.xls",
                },
                root_dir=str(TEST_DATA_DIR),
            ),
        ]
    elif dataset.name == "dof_421a":
        dataset.files = [
            nycdb.file.File(
                {"dest": "421a_2021_brooklyn.xlsx", "url": "https://example.com"},
                root_dir=str(TEST_DATA_DIR),
            )
        ]

    return dataset


def load_dataset(
    dataset: str, config: Config = Config(), force_check_urls: bool = False
):
    """
    Load the given dataset using the given configuration.

    Note that `force_check_urls` is only used by the test suite. This is a
    bad code smell, but unfortunately it was the easiest way to test the URL-checking
    functionality with test data.
    """

    if dataset == "wow":
        import wowutil

        wowutil.build(config.database_url)
        return
    elif dataset == "oca_address":
        import ocautil

        ocautil.build(config.database_url, config.use_test_data)
        return
    elif dataset == "signature":
        import signatureutil

        signatureutil.build(config.database_url, config.use_test_data)
        return
    elif dataset == "good_cause_eviction":
        import goodcauseutil

        goodcauseutil.build(config.database_url)
        return

    tables = get_tables_for_dataset(dataset)
    ds = Dataset(dataset, args=config.nycdb_args)
    ds = reset_files_if_test(ds, config)
    ds.setup_db()
    conn = ds.db.conn

    url_dbhash = get_url_dbhash(conn)
    modtracker = UrlModTracker(get_urls_for_dataset(dataset), url_dbhash)

    dataset_dbhash = get_dataset_dbhash(conn)
    dataset_tracker = DatasetTracker(dataset, dataset_dbhash)

    check_urls = (not config.use_test_data) or force_check_urls
    if check_urls and not modtracker.did_any_urls_change() and ds.files:
        slack.sendmsg(
            f"The dataset `{dataset}` has not changed since we last retrieved it."
        )
        return

    slack.sendmsg(f"Downloading the dataset `{dataset}`...")
    ds.download_files()

    slack.sendmsg(
        f"Downloaded the dataset `{dataset}`. Loading it into the database..."
    )
    temp_schema = create_temp_schema_name(dataset)
    with create_and_enter_temporary_schema(conn, temp_schema):
        ds.db_import()
        with save_and_reapply_permissions(conn, tables, "public"):
            drop_tables_if_they_exist(conn, tables, "public")
            change_table_schemas(conn, tables, temp_schema, "public")

    # The dataset's tables are ready, but any functions defined by the
    # dataset's custom SQL were in the temporary schema that just got
    # destroyed. Let's re-run only the function-creating SQL for the
    # dataset now, in the public schema so that clients can use it.
    run_sql_if_nonempty(conn, get_all_create_function_sql_for_dataset(dataset))

    modtracker.update_lastmods()
    dataset_tracker.update_tracker()
    slack.sendmsg(f"Finished loading the dataset `{dataset}` into the database.")
    print("Success!")


def init_rollbar():
    if ROLLBAR_ACCESS_TOKEN:
        print("Initializing Rollbar.")
        rollbar.init(
            access_token=ROLLBAR_ACCESS_TOKEN,
            environment="production",
            root=str(MY_DIR),
            handler="blocking",
        )


@contextmanager
def error_handling(dataset: str):
    init_rollbar()
    try:
        yield
    except Exception as e:
        if ROLLBAR_ACCESS_TOKEN:
            rollbar.report_exc_info(extra_data={"dataset": dataset})
        slack.sendmsg(
            f"Alas, an error occurred when loading the dataset `{dataset}`.",
            stdout=not isinstance(e, CommandError),
        )
        if isinstance(e, CommandError):
            print(e.message)
            sys.exit(1)
        else:
            raise


def main(argv: List[str] = sys.argv):
    dataset = os.environ.get("DATASET", "")

    if len(argv) > 1:
        dataset = argv[1]

    with error_handling(dataset):
        sanity_check()
        NYCDB_DATA_DIR.mkdir(parents=True, exist_ok=True)

        if not dataset:
            raise CommandError(
                f"Usage: {argv[0]} <dataset>\n"
                f"Alternatively, set the DATASET environment variable."
            )

        load_dataset(dataset)


if __name__ == "__main__":
    main()
