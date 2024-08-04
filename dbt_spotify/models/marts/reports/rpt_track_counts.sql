with history as (
    select * from {{ ref("fct_played_track") }}
),

dim_album as (
    select * from {{ ref("dim_album") }}
),

dim_track as (
    select * from {{ ref("dim_track") }}
),

counts as (
    select
        track_id,
        count(*) as plays
    from history
    where
        date_diff('day', played_at, current_date) <= 30
    group by track_id
),

final as (
    select
        dim_track.track_name,
        dim_track.artists,
        dim_album.album,
        counts.plays
    from counts
    left join dim_track
        on counts.track_id = dim_track.track_id
    left join dim_album
        on dim_track.album_id = dim_album.album_id
    order by counts.plays desc, dim_track.track_name
)

select * from final
