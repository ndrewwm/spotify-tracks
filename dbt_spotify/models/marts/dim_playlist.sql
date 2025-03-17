select distinct
    playlist_id,
    playlist_name,
    owner_id,
    playlist_followers_total,
    min(added_at) as dt_created,
    max(added_at) as dt_most_recent_addition,
    count(*) as total_tracks,
    median(track_popularity) as median_popularity,
    avg(track_popularity) as mean_popularity,
    stddev(track_popularity) as sd_popularity
from {{ ref('stg_playlist_tracks') }}
group by 
    playlist_id,
    playlist_name,
    owner_id,
    playlist_followers_total
