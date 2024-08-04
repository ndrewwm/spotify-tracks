with tracks_played as (
    select * from {{ ref("stg_recent_tracks") }}
),

dim_track as (
    select * from {{ ref("stg_dim_track") }}
),

combined as (
    select
        d.track_id,
        d.album_id,
        p.played_at,
        p.track_popularity,
        p.context
    from tracks_played p
    left join dim_track d
        on 
            p.track_name = d.track_name
            and p.track_album = d.album
            and p.track_artists = d.artists

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['track_id', 'played_at']) }} as play_id,
        combined.*
    from combined
)

select * from final
