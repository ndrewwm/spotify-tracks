select
    track_id,
    album_id,
    track_name,
    artists,
    duration_ms
from {{ ref("stg_dim_track") }}
