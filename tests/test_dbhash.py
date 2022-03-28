import pytest

from lib.dbhash import AbstractDbHash, DictDbHash, SqlDbHash


def _test_dbhash_implementation(dbh: AbstractDbHash):
    with pytest.raises(KeyError):
        dbh["foo"]

    with pytest.raises(KeyError):
        del dbh["foo"]

    assert "foo" not in dbh
    assert dbh.get("foo") is None

    dbh["foo"] = "bar"
    assert "foo" in dbh
    assert dbh["foo"] == "bar"
    assert dbh.get("foo") == "bar"

    dbh["foo"] = "baz"
    assert "foo" in dbh
    assert dbh["foo"] == "baz"
    assert dbh.get("foo") == "baz"

    del dbh["foo"]
    assert "foo" not in dbh

    dbh.set_or_delete("foo", None)
    assert "foo" not in dbh
    dbh.set_or_delete("foo", "quux")
    assert dbh["foo"] == "quux"
    dbh.set_or_delete("foo", None)
    assert "foo" not in dbh


def test_sqlite_sqldbhash():
    from pathlib import Path
    import sqlite3

    dbfile = Path("test_sqlite_sqldbhash.db")
    if dbfile.exists():
        dbfile.unlink()
    conn = sqlite3.connect(str(dbfile))
    dbh = SqlDbHash(conn, "blarg")

    _test_dbhash_implementation(dbh)

    conn.close()
    dbfile.unlink()


def test_dictdbhash_impl():
    _test_dbhash_implementation(DictDbHash({}))


def test_dictdbhash_defaults_to_empty_dict():
    assert DictDbHash().d == {}


def test_postgres_sqldbhash(conn):
    dbh = SqlDbHash(conn, "blarg")

    _test_dbhash_implementation(dbh)
