with sessions as (
    select
        concat(coalesce(cast(user_id as string), 'unknown'), '-', cast(session_id as string)) as user_session_id,
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
    from {{ ref('stg_events') }}
    where 1=1
        {{ get_incremental_filter(
            source_partition_date='date(event_created_at)',
            target_partition_date='date(session_started_at)'
        ) }}
    group by session_id, user_id
)

select * from sessions