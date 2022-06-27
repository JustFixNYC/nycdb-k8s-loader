import itertools
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, List

import psycopg2
import yaml
from sshtunnel import SSHTunnelForwarder

from load_dataset import Config

WOW_DIR = Path("/who-owns-what")

WOW_SQL_DIR = Path(WOW_DIR / "sql")

WOW_YML = yaml.load((WOW_DIR / "who-owns-what.yml").read_text(), Loader=yaml.FullLoader)

OCA_TABLES: List[str] = WOW_YML["oca_tables"]


@contextmanager
def oca_db_connect(config: Config = Config()):

    server = SSHTunnelForwarder(
        (config.oca_args.ssh_host, 22),
        ssh_username=config.oca_args.ssh_user,
        ssh_pkey=config.oca_args.ssh_pkey,
        remote_bind_address=(config.oca_args.db_host, 5432),
    )

    server.start()

    try:
        with psycopg2.connect(
            host=server.local_bind_host,
            database=config.oca_args.db_name,
            user=config.oca_args.db_user,
            password=config.oca_args.db_password,
            port=server.local_bind_port,
        ) as conn:
            yield conn
    finally:
        server.stop()


def create_oca_table(wow_conn, table_name: str):
    with wow_conn.cursor() as wow_cur:
        sql = (WOW_SQL_DIR / f"create_{table_name}.sql").read_text()
        wow_cur.execute(sql)


def iter_oca_rows(conn, table_name: str) -> Iterator[tuple]:
    with conn.cursor() as cur:
        sql = (WOW_SQL_DIR / f"select_{table_name}.sql").read_text()
        cur.execute(sql)
        yield cur.fetchall()


def grouper(n: int, iterable: Iterable[tuple]) -> Iterator[List[tuple]]:
    # https://stackoverflow.com/a/8991553

    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def populate_oca_table(oca_conn, wow_conn, table_name: str, batch_size=5000):
    with wow_conn.cursor() as wow_cur:
        for chunk in grouper(batch_size, iter_oca_rows(oca_conn, table_name)):
            args_str = b",".join(
                wow_cur.mogrify("(" + ", ".join(["%s"] * len(row)) + ")", row)
                for row in chunk
            ).decode()
            wow_cur.execute(f"INSERT INTO {table_name} VALUES {args_str}")


def create_and_populate_oca_tables(wow_conn, config: Config = Config()):
    with oca_db_connect(config) as oca_conn:
        for oca_table in OCA_TABLES:
            print(f"Creating {oca_table}...")
            create_oca_table(wow_conn, oca_table)
            populate_oca_table(oca_conn, wow_conn, oca_table)
    wow_conn.commit()
