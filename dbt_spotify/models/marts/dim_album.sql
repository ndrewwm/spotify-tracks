with tracks as (
    select * from {{ ref("stg_recent_tracks") }}
)

select distinct
    album_id,
    track_album as album,
    album_release_date as release_date
from tracks
order by album_id
