select
    {{
        dbt_utils.generate_surrogate_key(
            ['track_name', 'track_album', 'album_release_date', 'track_artists']
        )
    }} as track_id,
    {{
        dbt_utils.generate_surrogate_key(
            ['track_album', 'album_release_date']
        )
    }} as album_id,
    {{ dbt_utils.generate_surrogate_key(['track_artists']) }} as artist_id,
    track_name,
    track_album,
    track_artists,
    cast(
        case release_date_precision
            when 'day' then album_release_date
            when 'year' then album_release_date || '-01-01' 
        end as date
    ) as album_release_date,
    cast(track_popularity as tinyint) as track_popularity,
    duration_ms,
    cast(added_at as timestamp) as added_at,
    {{
        dbt_utils.generate_surrogate_key(["owner_id", "playlist_name"])
    }} as playlist_id,
    owner_id,
    playlist_name,
    playlist_followers_total
from {{ source('spotify', 'src_playlist_tracks') }}
