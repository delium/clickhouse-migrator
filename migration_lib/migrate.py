import time
import hashlib
import os
import pathlib
import json
import datetime

import pandas as pd
from clickhouse_driver import Client


def execute_and_inflate(client, query):
  result = client.execute(query, with_column_types=True)
  column_names = [c[0] for c in result[len(result) - 1]]
  return pd.DataFrame([dict(zip(column_names, d)) for d in result[0]])


def get_connection(db_name, db_host, db_user, db_password, db_port=None):
  return Client(db_host, port=db_port, user=db_user, password=db_password, database=db_name)


def init_db(client, db_name):
  client.execute("CREATE TABLE IF NOT EXISTS schema_versions (version UInt32, md5 String, script String, created_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY tuple(created_at)")


def migrations_to_apply(client, incoming):
  current_versions = execute_and_inflate(client, "SELECT version AS version, script AS c_script, md5 as c_md5 from schema_versions")
  if current_versions.empty:
    return incoming
  if (len(incoming) == 0 or len(incoming) < len(current_versions)):
    raise AssertionError(f"Migrations have gone missing, your code base should not truncate migrations, use migrations to correct older migrations")
  current_versions = current_versions.astype({'version': 'int32'})
  incoming = incoming.astype({'version': 'int32'})
  execution_stat = pd.merge(current_versions, incoming, on='version', how='outer')
  committed_and_absconded = execution_stat[execution_stat.c_md5.notnull() & execution_stat.md5.isnull()]
  if (len(committed_and_absconded) > 0):
    raise AssertionError(f"Migrations have gone missing, your code base should not truncate migrations, use migrations to correct older migrations")
  terms_violated = execution_stat[execution_stat.c_md5.notnull() & execution_stat.md5.notnull() & ~(execution_stat.md5 == execution_stat.c_md5)]
  if (len(terms_violated) > 0):
    raise AssertionError(f"Do not edit migrations once run, use migrations to correct older migrations")
  return execution_stat[execution_stat.c_md5.isnull()][['version', 'script', 'md5']]


def apply_migration(client, migrations, db_name, queue_exec=True):
  if (migrations.empty):
    return
  migrations = migrations.sort_values('version')
  for _, row in migrations.iterrows():
    with open(row['script']) as f:
      migration_scripts = json.load(f) if row['script'].endswith('.json') else [f.read()]
      for migration_script in migration_scripts:
        pipelined(client, migration_script, db_name) if queue_exec else client.execute(migration_script)
      print(f"INSERT INTO schema_versions(version, script, md5) VALUES({row['version']}, '{row['script']}', '{row['md5']}')")
      client.execute(f"INSERT INTO schema_versions(version, script, md5) VALUES", [{'version': row['version'], 'script': row['script'], 'md5':row['md5']}])

def pipelined(client, migration_script, db_name, timeout=60*60):
  ct = datetime.datetime.now()
  current_time=ct.strftime("%Y-%m-%d %H:%M:%S")
  client.execute(migration_script)
  while True:
    loop_time = datetime.datetime.now()
    if((loop_time - ct).total_seconds() >= timeout):
      raise Exception(f'Transaction Timeout - Unable to complete in {timeout} seconds, migration -> {migration_script}', )
    mutations_to_inspect = execute_and_inflate(client, f"SELECT database, table, mutation_id, lower(command) as command FROM system.mutations WHERE database='{db_name}' and create_time >= '{current_time}' and is_done=0")
    if mutations_to_inspect.empty:
      break
    mutations_to_inspect['match'] = mutations_to_inspect.apply(lambda row: row['command'] in migration_script, axis=1)
    mutations_to_inspect = mutations_to_inspect[mutations_to_inspect['match'] == True]
    if mutations_to_inspect.empty:
      break
    time.sleep(5)


def create_db(db_name, db_host, db_user, db_password, db_port=None):
  client = Client(db_host, port=db_port, user=db_user, password=db_password)
  client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
  client.disconnect()

def migrate(db_name, migrations_home, db_host, db_user, db_password, db_port=None, create_db_if_no_exists=True, queue_exec=True):
  if create_db_if_no_exists:
    create_db(db_name, db_host, db_user, db_password, db_port=db_port)
  client = get_connection(db_name, db_host, db_user, db_password, db_port=db_port)
  init_db(client, db_name)
  migrations = [{"version": int(f.name.split('_')[0].replace('V', '')),
                 "script": f"{migrations_home}/{f.name}", "md5": hashlib.md5(pathlib.Path(f"{migrations_home}/{f.name}").read_bytes()).hexdigest()}
                for f in os.scandir(f"{migrations_home}") if f.name.endswith('.sql') or f.name.endswith('.json')]
  apply_migration(client, migrations_to_apply(client, pd.DataFrame(migrations)), db_name, queue_exec=queue_exec)
  client.disconnect()
