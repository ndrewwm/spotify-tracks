with tracks as (
    select * from {{ ref("stg_recent_tracks") }}
),

summary as (
    select
        artist_id,
        count(*) as total_plays,
        min(played_at_mtn) as dttm_first_played
    from tracks
    group by artist_id
)

select distinct
    tr.artist_id,
    tr.track_artists as artists,
    su.total_plays,
    su.dttm_first_played,
    year(su.dttm_first_played::date) as yr_first_played,
    week(su.dttm_first_played::date) as wk_first_played
from tracks tr
left join summary su
    on tr.artist_id = su.artist_id
order by tr.artist_id
