## Clickhouse Migrator

[Clickhouse](https://clickhouse.tech/) is known for its scale to store and fetch large datasets.

Development and Maintenance of large-scale db systems many times requires constant changes to the actual DB system.
Holding off the scripts to migrate these will be painful.

We found there is nothing existing earlier and developed one inspired by, [Flyway](https://flywaydb.org/), [Alembic](https://alembic.sqlalchemy.org/en/latest/)

This is a python library, which you can execute as a pre-hook using sys python.
Or as a migration framework before deployment/server-startup in your application as required.


### Publishing to pypi
* python setup.py sdist
* twine upload dist/*


### Installation

You can install from pypi using `pip install clickhouse-migrator`.

### Usage

```python
from migration_lib.migrate import migrate

migrate(db_name, migrations_home, db_host, db_user, db_password, create_db_if_no_exists)
```

Parameter | Description | Default
-------|-------------|---------
db_name| Clickhouse database name | None
migrations_home | Path to list of migration files | <project_root>
db_host | Clickhouse database hostname | localhost
db_password | ***** | ****
create_db_if_no_exists | If the `db_name` is not present, enabling this will create the db | True

### Folder and Migration file patterns

The filenames are pretty similar to how `flyway` keeps it.

Your first version filename should be prefixed with `V1__` (double underscore)
These migrations are executed one by one, failures in between will stop and not further version files will be executed.

#### Multi statement and single statement migrations

If your migration is a single statement, you can create a file in the migration folder using the .sql extension and push your migration statement in there.

If you want to execute more than one statement in your migration, you can use a json file using the array syntax. Note that when using a json file, contents should be a valid json array as show. Ensure to keep migrations logical. Its not a good practise to push all migrations to one json file and neither is it wise to in all cases have them each statement in one file.


```json
[
  "CREATE TABLE pytest.sample1(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple() ORDER BY tuple()",
  "CREATE TABLE pytest.sample2(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple() ORDER BY tuple()",
  "CREATE TABLE pytest.sample3(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple() ORDER BY tuple()"
]
```
