with tracks_played as (
    select * from {{ ref("stg_recent_tracks") }}
),

final as (
    select
        play_id,
        track_id,
        album_id,
        played_at,
        played_at_mtn,
        track_popularity,
        context
    from tracks_played
    order by played_at_mtn desc
)

select * from final
