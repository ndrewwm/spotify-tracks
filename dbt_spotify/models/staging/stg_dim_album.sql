select distinct
    {{ dbt_utils.generate_surrogate_key(['track_album', 'album_release_date']) }} as album_id,
    track_album as album,
    album_release_date as release_date
from {{ ref("stg_recent_tracks") }}
