with events as (
    select * from {{ ref('stg_events') }}

    {% if is_incremental() %}
    where event_created_at >= timestamp_sub(current_timestamp(), interval 3 day)
    {% endif %}
),

sessions as (
    select
        session_id,
        user_id,
        min(event_created_at) as session_started_at,
        max(event_created_at) as session_ended_at,
        timestamp_diff(
            max(event_created_at),
            min(event_created_at),
            second
        ) as session_duration_seconds,
        count(*) as number_of_events,
        min(case when sequence_number = 1 then uri end) as entry_uri,
        max_by(uri, event_created_at) as exit_uri,
        min(browser) as browser,
        min(traffic_source) as traffic_source,
        min(city) as city,
        min(state) as state
    from events
    group by session_id, user_id
)

select * from sessions
