from pathlib import Path

import pandas as pd
import pytest

from clickhouse_migrations.migrate import execute_and_inflate, migrations_to_apply
from clickhouse_migrations.migrator import Migrator

TESTS_DIR = Path(__file__).parent


@pytest.fixture
def migrator():
    return Migrator("localhost", "default", "")


@pytest.fixture(autouse=True)
def before(migrator):
    clean_slate(migrator)


def clean_slate(migrator):
    with migrator.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest")
        conn.execute("CREATE DATABASE pytest")

    with migrator.connection("pytest") as conn:
        migrator.init_schema(conn)


def test_should_compute_no_migrations_to_run(migrator):
    with migrator.connection("pytest") as conn:
        incoming = pd.DataFrame([])
        results = migrations_to_apply(conn, incoming)
        assert results.size == 0


def test_should_raise_exception_on_deleted_migrations_no_incoming(migrator):
    incoming = pd.DataFrame([])
    with migrator.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )
        with pytest.raises(AssertionError):
            migrations_to_apply(conn, incoming)


def test_should_raise_exceptions_on_missing_migration(migrator):
    with migrator.connection("pytest") as conn:
        incoming = pd.DataFrame(
            [{"version": 2, "script": "location_to_script", "md5": "12345"}]
        )
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )
        with pytest.raises(AssertionError):
            migrations_to_apply(conn, incoming)


def test_should_raise_exceptions_on_modified_post_committed_migrations(migrator):
    with migrator.connection("pytest") as conn:
        incoming = pd.DataFrame(
            [{"version": 1, "script": "location_to_script", "md5": "12345"}]
        )
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )
        with pytest.raises(AssertionError):
            migrations_to_apply(conn, incoming)


def test_should_return_migrations_to_run(migrator):
    with migrator.connection("pytest") as conn:
        incoming = pd.DataFrame(
            [
                {"version": 1, "script": "location_to_script", "md5": "1234"},
                {"version": 2, "script": "location_to_script_2", "md5": "1234"},
            ]
        )
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )
        results = migrations_to_apply(conn, incoming)
        assert len(results) == 1
        assert results.version.values[0] == 2


def test_should_migrate_empty_database(migrator):
    clean_slate(migrator)

    with migrator.connection("pytest") as conn:
        tables = execute_and_inflate(conn, "show tables")
        assert len(tables) == 1
        assert tables.name.values[0] == "schema_versions"

        migrator.migrate("pytest", TESTS_DIR / "migrations")

        tables = execute_and_inflate(conn, "show tables")
        assert len(tables) == 2
        assert tables.name.values[0] == "sample"
        assert tables.name.values[1] == "schema_versions"
