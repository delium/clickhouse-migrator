import pandas as pd


def execute_and_inflate(client, query):
    result = client.execute(query, with_column_types=True)
    column_names = [c[0] for c in result[len(result) - 1]]
    return pd.DataFrame([dict(zip(column_names, d)) for d in result[0]])


def migrations_to_apply(client, incoming):
    current_versions = execute_and_inflate(
        client,
        "SELECT version AS version, script AS c_script, md5 as c_md5 from schema_versions",
    )
    if current_versions.empty:
        return incoming
    if len(incoming) == 0 or len(incoming) < len(current_versions):
        raise AssertionError(
            "Migrations have gone missing, "
            "your code base should not truncate migrations, "
            "use migrations to correct older migrations"
        )

    current_versions = current_versions.astype({"version": "int32"})
    incoming = incoming.astype({"version": "int32"})
    exec_stat = pd.merge(current_versions, incoming, on="version", how="outer")
    committed_and_absconded = exec_stat[
        exec_stat.c_md5.notnull() & exec_stat.md5.isnull()
    ]
    if len(committed_and_absconded) > 0:
        raise AssertionError(
            "Migrations have gone missing, "
            "your code base should not truncate migrations, "
            "use migrations to correct older migrations"
        )

    index = (
        exec_stat.c_md5.notnull()
        & exec_stat.md5.notnull()
        & ~(exec_stat.md5 == exec_stat.c_md5)
    )
    terms_violated = exec_stat[index]
    if len(terms_violated) > 0:
        raise AssertionError(
            "Do not edit migrations once run, "
            "use migrations to correct older migrations"
        )
    return exec_stat[exec_stat.c_md5.isnull()][["version", "script", "md5"]]


def apply_migration(client, migrations):
    if migrations.empty:
        return

    migrations = migrations.sort_values("version")
    for _, row in migrations.iterrows():
        with open(row["script"], "r", encoding="utf-8") as f:
            migration_script = f.read()
            client.execute(migration_script)
            print(
                f"INSERT INTO schema_versions(version, script, md5) "
                f"VALUES({row['version']}, '{row['script']}', '{row['md5']}')"
            )
            client.execute(
                "INSERT INTO schema_versions(version, script, md5) VALUES",
                [
                    {
                        "version": row["version"],
                        "script": row["script"],
                        "md5": row["md5"],
                    }
                ],
            )
