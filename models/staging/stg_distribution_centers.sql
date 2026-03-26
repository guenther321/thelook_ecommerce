with source_data as (
    select
        id              as distribution_center_id,
        name            as distribution_center_name,
        latitude,
        longitude,
        distribution_center_geom
    from {{ source('thelook_ecommerce', 'distribution_centers') }}
)

select * from source_data
