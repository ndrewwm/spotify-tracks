with tracks as (
    select * from {{ ref("stg_recent_tracks") }}
),

dim_album as (
    select * from {{ ref("stg_dim_album") }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['track_name', 'album_id', 'track_artists']) }} as track_id,
    a.album_id,
    a.album,
    track_name,
    track_artists as artists,
    duration_ms
from tracks t
left join dim_album a
    on
        t.track_album = a.album
        and t.album_release_date = a.release_date
