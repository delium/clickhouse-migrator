import pytest
from migration_lib.migrate import migrate, get_connection, migrations_to_apply, init_db, execute_and_inflate
import pandas as pd


@pytest.fixture
def client():
  client = get_connection('default', 'localhost', 'default', '')
  client.execute('CREATE DATABASE IF NOT EXISTS pytest')
  client.disconnect()
  return get_connection('pytest', 'localhost', 'default', '')


@pytest.fixture(autouse=True)
def before(client):
  client.execute('DROP TABLE IF EXISTS schema_versions')
  init_db(client, 'pytest')


def clean_slate(client):
  client.execute('DROP DATABASE pytest')
  client.execute('CREATE DATABASE pytest')
  init_db(client, 'pytest')


def test_should_compute_no_migrations_to_run(client):
  incoming = pd.DataFrame([])
  results = migrations_to_apply(client, incoming)
  assert results.size == 0


def test_should_raise_exception_on_deleted_migrations_no_incoming(client):
  incoming = pd.DataFrame([])
  client.execute('INSERT INTO schema_versions(version, script, md5) VALUES', [{'version': 1, 'script': 'location_to_script', 'md5': '1234'}])
  with pytest.raises(AssertionError):
    migrations_to_apply(client, incoming)


def test_should_raise_exceptions_on_missing_migration(client):
  incoming = pd.DataFrame([{'version': 2, 'script': 'location_to_script', 'md5': '12345'}])
  client.execute('INSERT INTO schema_versions(version, script, md5) VALUES', [{'version': 1, 'script': 'location_to_script', 'md5': '1234'}])
  with pytest.raises(AssertionError):
    migrations_to_apply(client, incoming)


def test_should_raise_exceptions_on_modified_post_committed_migrations(client):
  incoming = pd.DataFrame([{'version': 1, 'script': 'location_to_script', 'md5': '12345'}])
  client.execute('INSERT INTO schema_versions(version, script, md5) VALUES', [{'version': 1, 'script': 'location_to_script', 'md5': '1234'}])
  with pytest.raises(AssertionError):
    migrations_to_apply(client, incoming)


def test_should_return_migrations_to_run(client):
  incoming = pd.DataFrame([{'version': 1, 'script': 'location_to_script', 'md5': '1234'}, {'version': 2, 'script': 'location_to_script_2', 'md5': '1234'}])
  client.execute('INSERT INTO schema_versions(version, script, md5) VALUES', [{'version': 1, 'script': 'location_to_script', 'md5': '1234'}])
  results = migrations_to_apply(client, incoming)
  assert len(results) == 1
  assert results.version.values[0] == 2


def test_should_migrate_empty_database(client):
  client = get_connection('pytest', 'localhost', 'default', '')
  clean_slate(client)
  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 1
  assert tables.name.values[0] == 'schema_versions'
  migrate('pytest', 'tests/clickhouse_migrations', 'localhost', 'default', '')
  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 2
  assert tables.name.values[0] == 'sample'
  assert tables.name.values[1] == 'schema_versions'
  client.disconnect()
