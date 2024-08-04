"""Basic web app to receive an authorization code from Spotify."""

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from secrets import token_hex


STATE = token_hex(16)

app = FastAPI()


@app.get("/")
def hello(client_id: str):
    return {"hello": client_id}


@app.get("/login")
def login(client_id: str):
    """Walks the user through the authorization workflow."""

    scope = "user-read-recently-played"
    base = "https://accounts.spotify.com/authorize?"
    query = (
        "response_type=code"
        + f"&client_id={client_id}"
        + f"&scope={scope}"
        + f"&redirect_uri=http://localhost:8000/callback"
        + f"&state={STATE}"
    )
    return RedirectResponse(url=base + query)


@app.get("/callback")
def callback(state: str, code: str | None, error: str | None):
    """Callback from the authorization workflow."""

    if state != STATE:
        raise ValueError("Mismatched state")

    if error:
        raise ValueError(error)

    print(code)
    return {"code": code}


# https --form accounts.spotify.com/api/token \
#     grant_type=authorization_code \
#     code=$AUTHCODE \
#     redirect_uri='http://localhost:8000/callback' \
#     client_id=$SPOT_CLIENT \
#     client_secret=$SPOT_SECRET
