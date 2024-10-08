"""Builds the dbt project in motherduck, and loads the downstream Turso database."""

import os
import sqlite3

import duckdb
import libsql_experimental as libsql
from dbt.cli.main import dbtRunner
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule


@task
def get_credentials() -> dict:
    """Pull in  credentials needed to interact with the databases."""

    creds = {
        "motherduck": Secret.load("db-motherduck-token").get(),
        "turso": {
            "url": Secret.load("db-turso-spotify-url").get(),
            "token": Secret.load("db-turso-spotify-token").get(),
        },
    }
    return creds


@task
def dbt_build(token: str) -> None:
    """Build the dbt project."""

    os.chdir("dbt_spotify")
    os.environ["DBT_SECRET_MOTHERDUCK_TOKEN"] = token
    dbt = dbtRunner()

    res_deps = dbt.invoke(args=["deps"])
    if not res_deps.success:
        raise res_deps.exception

    res_build = dbt.invoke(args=["build", "--exclude", "config.materialized:view"])
    if not res_build.success:
        raise res_build.exception

    return


@task
def pull_data(token: str) -> None:
    """Pull the data from motherduck, storing it spottmporarily in a SQLite database."""

    duck = duckdb.connect(f"md:my_db?motherduck_token={token}")
    duck.sql("attach 'spottmp.db' (type sqlite);")
    duck.sql(
        "create table spottmp.dim_artist as select * from my_db.spotify.dim_artist;"
    )
    duck.sql("create table spottmp.dim_album as select * from my_db.spotify.dim_album;")
    duck.sql("create table spottmp.dim_track as select * from my_db.spotify.dim_track;")
    duck.sql(
        "create table spottmp.fct_track_play as select * from my_db.spotify.fct_played_track;"
    )
    duck.sql(
        "create table spottmp.rpt_track_counts as select * from my_db.spotify.rpt_track_counts;"
    )
    duck.sql(
        "create table spottmp.rpt_artist_counts as select * from my_db.spotify.rpt_artist_counts;"
    )
    duck.sql(
        "create table spottmp.rpt_discovery_rates as select * from my_db.spotify.rpt_discovery_rate"
    )
    return


@task
def generate_ddl() -> tuple[list[str], list[str]]:
    """Dump the SQLite data to a .sql file, for execution in turso replica.
    Returns the DDL for execution."""

    get_run_logger().info("Dumping local sqlite statements...")
    db = sqlite3.connect("spottmp.db")

    with open("./dump.sql", mode="w") as file:
        for line in db.iterdump():
            file.write(f"{line}\n")

    creates = []
    inserts = []
    with open("./dump.sql", mode="r") as file:
        statements = file.readlines()
        for statement in statements:
            if statement[:6] == "CREATE":
                print(statement)
                creates.append(statement)
            if statement[:6] == "INSERT":
                inserts.append(statement)

    return creates, inserts


@task
def load_turso(creates: list[str], inserts: list[str], creds: dict[str, str]) -> None:
    """Load the turso database."""

    get_run_logger().info("Setting up turso replica...")
    turso = libsql.connect(
        "turso.db",
        sync_url=creds["turso"]["url"],
        auth_token=creds["turso"]["token"],
    )

    tables = [
        statement.replace("CREATE TABLE ", "").split("(")[0] for statement in creates
    ]
    for table in tables:
        get_run_logger().info("Deleting %s...", table)
        turso.execute(f"drop table if exists {table};")

    for create in creates:
        get_run_logger().info("Executing %s", create)
        turso.execute(create)

    get_run_logger().info("Inserting data...")
    values = "".join(inserts)
    turso.execute(values)

    turso.commit()
    turso.sync()
    return


@flow
def dbt_build_and_load_turso() -> None:
    """Flow to automate the orchestration of the dbt project and loading of turso db."""

    creds = get_credentials()
    dbt_build(creds["motherduck"])
    pull_data(creds["motherduck"])
    creates, inserts = generate_ddl()
    load_turso(creates, inserts, creds)


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ndrewwm/spotify-tracks.git",
        entrypoint="flows/dbt_build_and_load_turso.py:dbt_build_and_load_turso",
    ).deploy(
        name="spotify | dbt_build_and_load_turso",
        work_pool_name="Managed Compute",
        schedule=CronSchedule(cron="10 8,17 * * *", timezone="America/Denver"),
    )
