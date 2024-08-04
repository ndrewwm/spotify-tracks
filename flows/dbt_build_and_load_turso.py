"""Builds the dbt project in motherduck."""

import os
import subprocess
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

from dbt.cli.main import dbtRunner, dbtRunnerResult
import duckdb
import libsql_experimental as libsql


@task
def get_credentials() -> dict:
    """Pull in  credentials needed to interact with the databases."""
    
    creds = {
        "motherduck": Secret.load("db-motherduck-token").get(),
        "turso": {
            "url": Secret.load("db-turso-spotify-url").get(),
            "token": Secret.load("db-turso-spotify-token").get(),
        }
    }
    return creds


@task
def dbt_build(creds: dict) -> None:
    """Build the dbt project."""

    os.environ["DBT_SECRET_MOTHERDUCK_TOKEN"] = creds["motherduck"]
    res: dbtRunnerResult = dbtRunner().invoke(
        args=[
            "build",
            "--project-dir",
            "./dbt_spotify/",
            "--exclude",
            "config.materialized:view",
        ],
    )

    if res.success:
        return

    raise res.exception


@task
def load_turso(creds: dict[str, str]) -> None:
    """Load the turso database."""

    ### PULL ###

    get_run_logger().info("Pulling tables from motherduck...")
    duck = duckdb.connect(f"md:my_db?motherduck_token={creds['motherduck']}")
    duck.sql("attach 'libsql_spotify.db' (type sqlite);")
    duck.sql("create table libsql_spotify.dim_artist as select * from my_db.spotify.dim_artist;")
    duck.sql("create table libsql_spotify.dim_album as select * from my_db.spotify.dim_album;")
    duck.sql("create table libsql_spotify.dim_track as select * from my_db.spotify.dim_track;")
    duck.sql("create table libsql_spotify.fct_track_play as select * from my_db.spotify.fct_played_track;")
    duck.sql("create table libsql_spotify.rpt_track_counts as select * from my_db.spotify.rpt_track_counts;")


    ### DUMP ###

    get_run_logger().info("Dumping local sqlite statements...")
    with open("./dump.sql", mode="w") as file:
        subprocess.call(
            args=["sqlite3", "libsql_spotify.db", ".dump"],
            stdout=file,
        )

    with open("./dump.sql", mode="r") as file:
        statements = file.readlines()
        creates = []
        inserts = []
        for statement in statements:
            if statement[:6] == "CREATE":
                print(statement)
                creates.append(statement)
            if statement[:6] == "INSERT":
                inserts.append(statement)

    ### LOAD ###

    get_run_logger().info("Setting up turso replica...")
    turso = libsql.connect(
        "turso.db", 
        sync_url=creds["turso"]["url"],
        auth_token=creds["turso"]["token"],
    )

    tables = [
        stmt.replace("CREATE TABLE ", "").split("(")[0]
        for stmt in creates
    ]
    for table in tables:
        get_run_logger().info("Deleting %s...", table)
        turso.execute(f'drop table if exists {table};')

    for create in creates:
        get_run_logger().info("Executing %s", create)
        turso.execute(create)

    get_run_logger().info("Inserting data...")
    values = ''.join(inserts)
    turso.execute(values)

    turso.commit()
    turso.sync()
    return


@flow
def dbt_build_and_load_turso() -> None:
    """Flow to automate the orchestration of the dbt project and loading of turso db."""

    creds = get_credentials()
    dbt_build(creds)
    load_turso(creds)


if __name__ == "__main__":
    # dbt_build_and_load_turso()
    flow.from_source(
        source="https://github.com/ndrewwm/spotify-tracks.git",
        entrypoint="flows/dbt_build_and_load_turso.py:dbt_build_and_load_turso",
    ).deploy(
        name="spotify | dbt_build_and_load_turso",
        work_pool_name="Managed Compute",
        schedule=CronSchedule(cron="10 8,17 * * *", timezone="America/Denver"),
    )
