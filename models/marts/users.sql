with users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        -- primary key
        user_id,

        -- identity
        first_name,
        last_name,
        email,

        -- demographics
        age,
        gender,
        case
            when age < 18              then 'under_18'
            when age between 18 and 24 then '18_24'
            when age between 25 and 34 then '25_34'
            when age between 35 and 44 then '35_44'
            when age between 45 and 54 then '45_54'
            when age between 55 and 64 then '55_64'
            else '65_plus'
        end                             as age_band,

        -- location
        city,
        state,
        country,
        postal_code,
        latitude,
        longitude,

        -- acquisition
        traffic_source,
        user_created_at,
        date(user_created_at)           as user_created_date

    from users
)

select * from final