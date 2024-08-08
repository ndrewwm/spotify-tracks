with history as (
    select * from {{ ref("fct_played_track") }}
),

dim_track as (
    select * from {{ ref("dim_track") }}
),

counts as (
    select
        dim_track.artists,
        count(*) as plays
    from history
    inner join dim_track
        on history.track_id = dim_track.track_id
    where
        date_diff('day', played_at, current_date) <= 30
    group by dim_track.artists
    order by count(*) desc
)

select * from counts