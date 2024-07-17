"""\
Perform various operations on the database.

Usage:
  dbtool.py shell
  dbtool.py rowcounts <dataset>...
  dbtool.py lastmod:list <dataset>...
  dbtool.py lastmod:reset <dataset>...
  dbtool.py user:grant_schema_read <user> <schema>
  dbtool.py user:create <user>

Options:
  -h --help     Show this screen.

Environment variables:
  DATABASE_URL           The URL of the NYC-DB database.
"""

import os
import sys
from typing import List, Tuple, Iterator
import psycopg2
import docopt
import nycdb.dataset
from nycdb.utility import list_wrap
from urllib.parse import urlparse
import nycdb.cli

import load_dataset
from lib.lastmod import LastmodInfo


def get_tables_for_datasets(names: List[str]) -> List[str]:
    tables: List[str] = []

    for name in names:
        schema = list_wrap(nycdb.dataset.datasets()[name]["schema"])
        tables.extend([t["table_name"] for t in schema])

    return tables


def validate_and_get_dataset_names(dsnames: List[str]) -> List[str]:
    dataset_names: List[str] = []
    all_dataset_names = nycdb.dataset.datasets().keys()

    for name in dsnames:
        if name == "all":
            dataset_names.extend(all_dataset_names)
        elif name not in all_dataset_names:
            print(f"ERROR: {name} is not a valid dataset. Please choose from:")
            print("\n".join(all_dataset_names))
            print("all")
            sys.exit(1)
        else:
            dataset_names.append(name)

    return dataset_names


def get_rowcounts(
    conn, table_names: List[str], schema: str = "public"
) -> Iterator[Tuple[str, int]]:
    with conn.cursor() as cur:
        for table in table_names:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count: int = cur.fetchone()[0]
            yield table, count


def print_rowcounts(conn, table_names: List[str], schema: str = "public"):
    for table, count in get_rowcounts(conn, table_names, schema=schema):
        print(f"  {table} has {count:,} rows.")


def show_rowcounts(db_url: str, dataset_names: List[str]):
    with psycopg2.connect(db_url) as conn:
        for dataset in dataset_names:
            tables = get_tables_for_datasets([dataset])
            schemas = load_dataset.get_temp_schemas(conn, dataset)
            for schema in schemas:
                ts = load_dataset.get_friendly_temp_schema_creation_time(schema)
                print(f"For {dataset}'s temporary schema created on {ts}:\n")
                print_rowcounts(conn, tables, schema=schema)
                print()
            print(f"For {dataset}'s public schema:\n")
            print_rowcounts(conn, tables)


def shell(db_url: str):
    args = load_dataset.Config(database_url=db_url).nycdb_args
    nycdb.cli.run_dbshell(args)


def list_lastmod(db_url: str, dataset_names: List[str]):
    with psycopg2.connect(db_url) as conn:
        dbhash = load_dataset.get_url_dbhash(conn)
        for dataset in dataset_names:
            print(f"For the dataset {dataset}:")
            urls = load_dataset.get_urls_for_dataset(dataset)
            for url in urls:
                info = LastmodInfo.read_from_dbhash(url, dbhash)
                if info.last_modified:
                    print(f"  The URL {url} was last modified on {info.last_modified}.")
                else:
                    print(
                        f"  The URL {url} has no metadata about its last modification date."
                    )


def reset_lastmod(db_url: str, dataset_names: List[str]):
    with psycopg2.connect(db_url) as conn:
        dbhash = load_dataset.get_url_dbhash(conn)
        for dataset in dataset_names:
            print(f"For the dataset {dataset}:")
            urls = load_dataset.get_urls_for_dataset(dataset)
            for url in urls:
                info = LastmodInfo(url)
                print(f"Clearing last modification metadata for {dataset}'s URL {url}.")
                info.write_to_dbhash(dbhash)


def grant_schema_read(db_url: str, user: str, schema: str):
    print(f"Granting user '{user}' read-only access to schema '{schema}'.")
    alter_default_privs = f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}"
    statements = [
        f"GRANT USAGE ON SCHEMA {schema} TO {user}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {user}",
        f"{alter_default_privs} GRANT SELECT ON TABLES TO {user}",
        f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} to {user}",
        f"{alter_default_privs} GRANT EXECUTE ON FUNCTIONS TO {user}",
    ]
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def generate_random_password(num_chars=16) -> str:
    # https://docs.python.org/3/library/secrets.html
    import string
    import secrets

    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for i in range(num_chars))


def create_user(db_url: str, user: str):
    schema = "public"
    db = urlparse(db_url).path[1:]
    password = generate_random_password()
    print(f"Creating user '{user}' and granting them access to the '{db}' db.")
    print(f"Their password is '{password}'. Please keep this safe!")
    statements = [
        f"CREATE USER {user} WITH ENCRYPTED PASSWORD '{password}'",
        f"GRANT CONNECT ON DATABASE {db} TO {user}",
    ]
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
    grant_schema_read(db_url, user, schema)


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    dataset_names: List[str] = []
    if args.get("<dataset>"):
        dataset_names = validate_and_get_dataset_names(args["<dataset>"])

    if args["rowcounts"]:
        show_rowcounts(db_url, dataset_names)
    elif args["shell"]:
        shell(db_url)
    elif args["lastmod:list"]:
        list_lastmod(db_url, dataset_names)
    elif args["lastmod:reset"]:
        reset_lastmod(db_url, dataset_names)
    elif args["user:grant_schema_read"]:
        grant_schema_read(db_url, args["<user>"], args["<schema>"])
    elif args["user:create"]:
        create_user(db_url, args["<user>"])


if __name__ == "__main__":
    main(argv=sys.argv[1:], db_url=os.environ["DATABASE_URL"])
