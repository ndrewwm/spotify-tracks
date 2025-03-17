"""Pull playlist tracks, based on supplied playlist IDs"""

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
def check_token(token: str) -> tuple[bool, str]:
    """See if the current access token is still valid."""

    get_run_logger().info("Checking current access token...")
    req = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    return req.status_code != 200, token


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


def _get_details(playlist_id: str, token: str) -> dict[str, str | int]:
    """Get details about the playlist"""

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "market": "US",
        "fields": "name,owner(display_name,id,type),followers,description,href",
    }
    req = requests.get(
        url=f"https://api.spotify.com/v1/playlists/{playlist_id}",
        headers=headers,
        params=params,
        timeout=60,
    )
    req.raise_for_status()

    data: dict[str, dict[str, str | int] | str] = req.json()
    return {
        "owner_id": data.get("owner").get("id"),
        "owner_display_name": data.get("owner").get("display_name"),
        "playlist_name": data.get("name"),
        "playlist_href": data.get("href"),
        "playlist_followers_total": data.get("followers").get("total"),
    }


def _get_items(items: list[dict], playlist_details: dict[str, str | int]) -> list[dict]:
    """Helper function to clean up the nested tracks objects."""

    out = []
    for item in items:
        track: dict = item.get("track")
        track_name: str = track.get("name")
        album: dict = track.get("album")
        track: dict = item.get("track")
        track_name: str = track.get("name")
        album: dict = track.get("album")
        album_name: str = album.get("name")
        release_date: str = album.get("release_date")
        release_date_precision: str = album.get("release_date_precision")
        popularity: float = track.get("popularity")
        added_at: str = item.get("added_at")
        duration_ms: float = track.get("duration_ms")

        track_artists: list[dict] = track.get("artists")
        artists: list[str] = []
        for artist in track_artists:
            artists.append(artist.get("name"))

        out.append(
            {
                "track_name": track_name,
                "track_album": album_name,
                "track_artists": ", ".join(artists),
                "album_release_date": release_date,
                "release_date_precision": release_date_precision,
                "track_popularity": popularity,
                "duration_ms": duration_ms,
                "added_at": added_at,
                "playlist_name": playlist_details["playlist_name"],
                "owner_id": playlist_details["owner_id"],
                "playlist_followers_total": playlist_details[
                    "playlist_followers_total"
                ],
            }
        )

    return out


@task(retries=1, retry_delay_seconds=15)
def get_tracks(playlist_id: str, token: str) -> list[dict]:
    """Gather tracks from a playlist."""

    logger = get_run_logger()

    logger.info("Getting playlist details for playlist %s...", playlist_id)
    deets = _get_details(playlist_id, token)

    logger.info("Pulling tracks from playlist %s...", playlist_id)
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "limit": 100,
        "market": "US",
        "fields": ",".join(
            [
                "total",
                "offset",
                "next",
                "items(added_at,track(name,artists(name),album(name,release_date,release_date_precision),duration_ms,popularity)",
            ]
        ),
    }
    req = requests.get(
        url=f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        headers=headers,
        params=params,
        timeout=60,
    )
    req.raise_for_status()

    data = req.json()
    out = []
    out.extend(_get_items(data["items"], deets))

    if data["next"]:
        while len(out) < data["total"]:
            next = data["next"]
            logger.info("GET %s", next)
            req = requests.get(next, headers=headers, timeout=60)
            req.raise_for_status()

            data = req.json()
            out.extend(_get_items(data["items"], deets))
            time.sleep(3)

    logger.info("Total # of tracks: %s", len(out))
    return out


@task(retries=1, retry_delay_seconds=15)
def get_db(token: str):
    """Return a database connection to the motherduck db."""

    return duckdb.connect(f"md:?motherduck_token={token}")


@task(cache_policy=None)
def insert_data(conn: DuckDBPyConnection, data: list[dict]) -> None:
    """Load gathered tracks into the database."""

    get_run_logger().info("Truncating table...")
    conn.execute("truncate table spotify.src_playlist_tracks;")

    get_run_logger().info("Attempting to insert %s rows...", len(data))
    rows = [tuple(item.values()) for item in data]
    conn.executemany(
        query="""
            insert into spotify.src_playlist_tracks(
              track_name, track_album, track_artists, album_release_date,
              release_date_precision, track_popularity, duration_ms, added_at,
              playlist_name, owner_id, playlist_followers_total
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        parameters=rows,
    )


@flow
def get_treefort_playlists(
    playlist_ids: list[str] = [
        "7l6kigtJAwAQK9Wq2oV0RE",
        "7m8TauNcJ578a5EKoW4WiE",
        "2jgNvluBYxt8nYQ5HZ0HiC",
    ],
):
    """Retrieve tracks from provided playlists, upload to MotherDuck."""

    creds = get_credentials()
    token_expired, token = check_token(creds["access_token"])
    if token_expired:
        token, refresh = get_token(creds["spotify_body"])
        store_tokens(token, refresh)

    tracks = []
    for id in playlist_ids:
        tracks.extend(get_tracks(id, creds["access_token"]))

    conn = get_db(creds["md_token"])
    insert_data(conn, tracks)


if __name__ == "__main__":
    get_treefort_playlists()
