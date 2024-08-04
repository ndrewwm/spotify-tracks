"""Gather recently played tracks from the Spotify API."""

import datetime
import time
import requests
import duckdb
from duckdb import DuckDBPyConnection
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule


@task
def get_credentials() -> dict[str, dict[str, str]]:
    """Pull needed secrets from Prefect."""

    creds = {}
    creds["access_token"] = Secret.load("api-spotify-access-token").get()
    creds["spotify_body"] = {
        "grant_type": "refresh_token",
        "refresh_token": Secret.load("api-spotify-refresh-token").get(),
        "client_id": Secret.load("api-spotify-client-id").get(),
        "client_secret": Secret.load("api-spotify-secret").get(),
    }
    creds["md_token"] = Secret.load("db-motherduck-token").get()

    return creds


@task
def get_unix_timestamp() -> float:
    """Get the ceiling for requests we'll make today."""

    today = datetime.datetime.today()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    unix = time.mktime(today.timetuple())
    return unix


@task
def check_token(token: str) -> tuple[bool, str]:
    """See if the current access token is still valid."""

    get_run_logger().info("Checking current access token...")
    req = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    return req.status_code == 200, token


@task
def get_token(creds: dict[str, str]) -> tuple[str, str]:
    """Retrieves an access token from the Spotify API."""

    get_run_logger().info("Exchanging current refresh token for fresh access token...")
    req = requests.post(
        url="https://accounts.spotify.com/api/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=creds,
    )
    req.raise_for_status()

    data: dict[str, str] = req.json()
    get_run_logger().info(data)
    return data.get("access_token"), data.get("refresh_token")


@task
def store_tokens(access: str, refresh: str) -> None:
    """Updates the secrets in Prefect Cloud."""

    Secret(value=access).save(name="api-spotify-access-token", overwrite=True)
    # Secret(value=refresh).save(name="api-spotify-refresh-token", overwrite=True)
    return


def _get_items(items: list[dict]) -> list[dict]:
    """Helper function to pull out relevant fields from the recently played tracks."""

    out = []
    for item in items:
        track: dict = item.get("track")
        track_name: str = track.get("name")
        album: dict = track.get("album")
        album_name: str = album.get("name")
        release_date: str = album.get("release_date")
        release_date_precision: str = album.get("release_date_precision")
        popularity: float = track.get("popularity")
        played_at: str = item.get("played_at")
        
        context: dict = item.get("context")
        if context:
            context = context.get("type")
        
        duration_ms: float = track.get("duration_ms")

        track_artists: list[dict] = track.get("artists")
        artists: list[str] = []
        for artist in track_artists:
            artists.append(artist.get("name"))

        out.append({
            "track_name": track_name,
            "track_album": album_name,
            "track_artists": ", ".join(artists),
            "album_release_date": release_date,
            "release_date_precision": release_date_precision,
            "track_popularity": popularity,
            "played_at": played_at,
            "context": context if context else None,
            "duration_ms": duration_ms,
        })

    return out


@task
def get_tracks(token: str, today: float) -> dict:
    """Gather tracks from the Spotify API."""

    logger = get_run_logger()
    logger.info("Pulling recent tracks...")
    
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": 50}
    req = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played",
        headers=headers,
        params=params,
        timeout=60,
    )
    req.raise_for_status()

    data = req.json()
    out = []
    out.extend(_get_items(data["items"]))

    if data["next"]:
        next = data["next"]
        j = 0
        while next and j < 5:
            logger.info("GET %s", next)
            req = requests.get(next, headers=headers, params=params, timeout=60)
            req.raise_for_status()

            data = req.json()
            out.extend(_get_items(data["items"]))
            next = data["next"]

            # Prepare for next iteration
            logger.info("j=%s", j)
            time.sleep(3)
            j += 1

    logger.info("Total # tracks: %s", len(out))
    return out


@task
def prep_data(data: dict) -> dict:
    """Clean up the data from the API."""


@task
def get_db(token: str):
    """Return a database connection to the motherduck db."""

    return duckdb.connect(f"md:?motherduck_token={token}")


@task
def insert_data(conn: DuckDBPyConnection, data: list[dict]) -> None:
    """Load gathered tracks into the database."""

    get_run_logger().info("Attempting to insert %s rows...", len(data))
    rows = [tuple(item.values()) for item in data]
    conn.executemany(
        query="""
            insert into spotify.src_recent_tracks (
              track_name, track_album, track_artists, album_release_date,
              release_date_precision, track_popularity, played_at, context, duration_ms
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict do nothing;
        """,
        parameters=rows,
    )


@flow
def pull_recent_tracks() -> None:
    """Flow to gather recent spotify tracks."""

    creds = get_credentials()
    today = get_unix_timestamp.submit()

    token_valid, token = check_token(creds["access_token"])
    if not token_valid:
        token, refresh = get_token(creds["spotify_body"])
        store_tokens.submit(token, refresh)

    data = get_tracks.submit(token, today)
    conn = get_db.submit(creds["md_token"])
    insert_data.submit(conn, data)


if __name__ == "__main__":
    # pull_recent_tracks()
    flow.from_source(
        source="https://github.com/ndrewwm/spotify-tracks.git",
        entrypoint="flows/pull_recent_tracks.py:pull_recent_tracks",
    ).deploy(
        name="spotify | pull_recent_tracks",
        work_pool_name="Managed Compute",
        schedule=CronSchedule(cron="30 8-23/2 * * *", timezone="America/Denver")
    )
