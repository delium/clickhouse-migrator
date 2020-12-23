## Clickhouse Migrator

[Clickhouse](https://clickhouse.tech/) is known for its scale to store and fetch large datasets.

Development and Maintenance of large-scale db systems many times requires constant changes to the actual DB system.
Holding off the scripts to migrate these will be painful.

We found there is nothing existing earlier and developed one inspired by, [Flyway](https://flywaydb.org/), [Alembic](https://alembic.sqlalchemy.org/en/latest/)

This is a python library, which you can execute as a pre-hook using sys python.
Or as a migration framework before deployment/server-startup in your application as required.


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
