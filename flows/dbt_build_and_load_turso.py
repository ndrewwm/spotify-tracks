"""Builds the dbt project in motherduck, and loads the downstream Turso database."""

import os
import sqlite3

import requests
import duckdb
from dbt.cli.main import dbtRunner
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
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

    duck = duckdb.connect(f"md:my_db?motherduck_token={token}", read_only=True)
    duck.sql("attach 'spottmp.db' (type sqlite);")
    duck.sql(
        """drop table if exists spottmp.dim_artist;
           drop table if exists spottmp.dim_album;
           drop table if exists spottmp.dim_track;
           drop table if exists spottmp.fct_track_play;
           drop table if exists spottmp.rpt_track_counts;
           drop table if exists spottmp.rpt_artist_counts;
           drop table if exists spottmp.rpt_discovery_rates;"""
    )
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
def generate_ddl() -> None:
    """Dump the SQLite data to a .sql file, for execution against the turso db."""

    get_run_logger().info("Dumping local sqlite statements...")
    db = sqlite3.connect("spottmp.db")

    with open("./dump.sql", mode="w") as file:
        for line in db.iterdump():
            file.write(f"{line}\n")
    return


@task
def read_ddl() -> tuple[list[str], list[str]]:
    """Returns the DDL statements, ready for execution"""

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
def turso_execute(stmt: str | list[str], creds: dict[str, str]):
    """Execute a statement against the turso database."""

    if not isinstance(stmt, list):
        stmt = [stmt]
    statements = [{"type": "execute", "stmt": {"sql": statement}} for statement in stmt]
    statements.append({"type": "close"})

    url = f"{creds['url'].replace('libsql://', 'https://')}/v2/pipeline"
    headers = {
        "Authorization": f"Bearer {creds['token']}",
        "Content-Type": "application/json",
    }
    payload = {"requests": statements}
    req = requests.post(url=url, headers=headers, json=payload, timeout=60)
    req.raise_for_status()
    return req.json()


@flow
def turso_load(creds: dict[str, str]) -> None:
    """Load the turso database."""

    logger = get_run_logger()
    creates, inserts = read_ddl()
    tables = [
        statement.replace("CREATE TABLE ", "").split("(")[0] for statement in creates
    ]

    logger.info("Dropping tables...")
    turso_execute([f"drop table if exists {table}" for table in tables], creds)

    logger.info("Creating tables...")
    turso_execute(creates, creds)

    futures: list[PrefectFuture] = []
    for table in tables:
        logger.info("Inserting data into %s...", table)
        statements = [
            insert for insert in inserts if f'INSERT INTO "{table}"' in insert
        ]
        futures.append(turso_execute.submit(statements, creds))

    for future in futures:
        future.wait()

    return


@flow
def dbt_build_and_load_turso() -> None:
    """Flow to automate the orchestration of the dbt project and loading of turso db."""

    creds = get_credentials()
    dbt_build(creds["motherduck"])
    pull_data(creds["motherduck"])
    generate_ddl()
    turso_load(creds["turso"])


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ndrewwm/spotify-tracks.git",
        entrypoint="flows/dbt_build_and_load_turso.py:dbt_build_and_load_turso",
    ).deploy(
        name="spotify | dbt_build_and_load_turso",
        work_pool_name="Managed Compute",
        schedule=CronSchedule(cron="10 8,17 * * *", timezone="America/Denver"),
    )
