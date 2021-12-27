import pytest
from clickhouse_migrate.migrate import migrate, get_connection, migrations_to_apply, init_db, execute_and_inflate
import pandas as pd
import os


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

def test_should_migrate_using_sql_and_json_migrations(client):
  client = get_connection('pytest', 'localhost', 'default', '')
  clean_slate(client)
  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 1
  assert tables.name.values[0] == 'schema_versions'
  migrate('pytest', 'tests/migrations_mixed', 'localhost', 'default', '')
  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 5
  assert tables.name.values[0] == 'sample'
  assert tables.name.values[1] == 'sample1'
  assert tables.name.values[2] == 'sample2'
  assert tables.name.values[3] == 'sample3'
  assert tables.name.values[4] == 'schema_versions'
  client.disconnect()

def test_should_migrate_in_queue_when_enabled(client):
  client = get_connection('pytest', 'localhost', 'default', '')
  clean_slate(client)
  client.execute('CREATE TABLE sample(id UInt32, name UInt32) ENGINE MergeTree PARTITION BY tuple() ORDER BY tuple()')

  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 2
  assert tables.name.values[0] == 'sample'
  assert tables.name.values[1] == 'schema_versions'

  os.system('gunzip < "tests/migrations_seq/test.csv.gz" | clickhouse-client --query="INSERT INTO pytest.sample FORMAT CSVWithNames"')
  total_rows = 100000
  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample').nrow.values[0] == total_rows
  enabled0 = execute_and_inflate(client, "SELECT COUNT(*) AS nrow FROM pytest.sample WHERE name > 3000").nrow.values[0]

  migrate('pytest', 'tests/migrations_seq', 'localhost', 'default', '', queue_exec=True)

  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample WHERE enabled = 0').nrow.values[0] == enabled0
  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample WHERE guard = 0').nrow.values[0] == enabled0

  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample WHERE guard = 1').nrow.values[0] == total_rows - enabled0
  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample WHERE guard = -1').nrow.values[0] == 0

  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 2
  assert tables.name.values[0] == 'sample'
  assert tables.name.values[1] == 'schema_versions'
  client.disconnect()

def test_ensure_parallel_dataset_fail_on_no_queue(client):
  client = get_connection('pytest', 'localhost', 'default', '')
  clean_slate(client)
  client.execute('CREATE TABLE sample(id UInt32, name UInt32) ENGINE MergeTree PARTITION BY tuple() ORDER BY tuple()')

  tables = execute_and_inflate(client, 'show tables')
  assert len(tables) == 2
  assert tables.name.values[0] == 'sample'
  assert tables.name.values[1] == 'schema_versions'

  os.system('gunzip < "tests/migrations_seq/test.csv.gz" | clickhouse-client --query="INSERT INTO pytest.sample FORMAT CSVWithNames"')
  total_rows = 100000
  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample').nrow.values[0] == total_rows
  enabled0 = execute_and_inflate(client, "SELECT COUNT(*) AS nrow FROM pytest.sample WHERE name > 3000").nrow.values[0]

  migrate('pytest', 'tests/migrations_seq', 'localhost', 'default', '', queue_exec=False)

  assert execute_and_inflate(client, 'SELECT COUNT(*) AS nrow FROM pytest.sample WHERE guard = 0').nrow.values[0] != enabled0
  client.disconnect()
