with tracks as (
    select * from {{ ref("fct_played_track") }}
),

dim_track as (
    select * from {{ ref("dim_track") }}
),

dim_artist as (
    select * from {{ ref("dim_artist") }}
),

base as (
    select distinct
        year(played_at_mtn) as year_played,
        week(played_at_mtn) as week_played,
        max(played_at_mtn::date) as week_dt,
        count(*) as plays,
        count(distinct dt.artists) as uniq_artists,
        count(distinct dt.track_id) as uniq_tracks
    from tracks
    left join dim_track dt
        on tracks.track_id = dt.track_id
    group by year_played, week_played
    order by year_played, week_played
),

new_tracks as (
    select
        yr_first_played as year_played,
        wk_first_played as week_played,
        count(*) as new_tracks
    from dim_track
    group by yr_first_played, wk_first_played
),

new_artists as (
    select
        yr_first_played as year_played,
        wk_first_played as week_played,
        count(*) as new_artists
    from dim_artist
    group by yr_first_played, wk_first_played  
),

final as (
    select
        base.*,
        new_tracks.new_tracks,
        new_artists.new_artists,
        {{ 
            dbt_utils.safe_divide('new_tracks.new_tracks', 'base.uniq_tracks') 
        }} as track_discovery_rate,
        {{
            dbt_utils.safe_divide('new_artists.new_artists', 'base.uniq_artists')
        }} as artist_discovery_rate
    from base
    left join new_tracks
        on 
            base.year_played = new_tracks.year_played
            and base.week_played = new_tracks.week_played

    left join new_artists
        on 
            base.year_played = new_artists.year_played
            and base.week_played = new_artists.week_played
)

select * from final
