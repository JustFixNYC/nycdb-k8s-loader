from lib.parse_created_tables import (
    parse_created_tables,
    parse_nycdb_created_tables
)


def test_it_returns_empty_list_when_no_tables_are_created():
    assert parse_created_tables(
        "ALTER TABLE hpd_registrations ADD COLUMN bbl char(10);"
    ) == []


def test_it_parses_create_table_statements():
    sql = """\
    DROP TABLE IF EXISTS blarg;

    CREATE TABLE blarg
    as SELECT
    first(floof) as floof,
    FROM goopy
    GROUP BY bbl, jibjab;

    create index on blarg (floof);
    """

    assert parse_created_tables(sql) == ['blarg']
    assert parse_created_tables(sql.lower()) == ['blarg']
    assert parse_created_tables(sql.upper()) == ['BLARG']


def test_it_parses_create_table_statements_2():
    sql = """\
    CREATE TABLE  foo AS (
       SELECT BusinessHouseNumber,
       blah
       FROM thing
       WHERE boop IS NOT NULL;
    """

    assert parse_created_tables(sql) == ['foo']


def test_it_parses_nycdb_files():
    assert 'hpd_registrations_grouped_by_bbl' in parse_nycdb_created_tables([
        'hpd_registrations/registrations_grouped_by_bbl.sql'])
