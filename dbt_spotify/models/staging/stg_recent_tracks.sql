select
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
    context,
    duration_ms
from {{ source('spotify', 'src_recent_tracks') }}
