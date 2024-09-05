with tracks as (
    select * from {{ ref("stg_recent_tracks") }}
),

summary as (
    select
        track_id,
        count(*) as total_plays,
        min(played_at_mtn) as dttm_first_played
    from tracks
    group by track_id
)

select distinct
    tr.track_id,
    tr.album_id,
    tr.track_album as album,
    tr.track_name,
    tr.track_artists as artists,
    su.total_plays,
    su.dttm_first_played,
    year(su.dttm_first_played::date) as yr_first_played,
    week(su.dttm_first_played::date) as wk_first_played,
    tr.duration_ms
from tracks tr
left join summary su
    on tr.track_id = su.track_id
order by tr.track_id
