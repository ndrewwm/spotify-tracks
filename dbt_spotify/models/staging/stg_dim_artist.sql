with artists as (
    select distinct
        unnest(string_split(track_artists, ', ')) as artist
    from {{ ref("stg_recent_tracks") }}
)

select
    {{ dbt_utils.generate_surrogate_key(['artist']) }} as artist_id,
    artist
from artists
