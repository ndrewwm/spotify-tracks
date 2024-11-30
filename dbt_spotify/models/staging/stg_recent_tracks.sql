select
    {{ 
        dbt_utils.generate_surrogate_key(
            ['track_name', 'track_album', 'track_artists', 'played_at']
        )
    }} as play_id,
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
    cast(played_at as timestamp) as played_at,
    timezone(
        'America/Boise',
        cast(cast(played_at as timestamp) || '+00' as timestamptz)
    ) as played_at_mtn,
    context,
    duration_ms
from {{ source('spotify', 'src_recent_tracks') }}
