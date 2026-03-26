with source_data as (
    select
        id              as event_id,
        user_id,
        sequence_number,
        session_id,
        created_at      as event_created_at,
        ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri,
        event_type
    from {{ source('thelook_ecommerce', 'events') }}
)

select * from source_data
