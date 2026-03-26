with source_data as (
    select
        id              as user_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        created_at      as user_created_at,
        user_geom
    from {{ source('thelook_ecommerce', 'users') }}
)

select * from source_data
