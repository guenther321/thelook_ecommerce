{% snapshot snap_int_chargeback_transactions %}
  {%- set build_metadata = namespace(name='manual', version='1.0.0') -%}
  
  {{
    config(
      target_schema='snapshots',
      unique_key='external_ref',
      strategy='timestamp',
      updated_at='updated_at',
      partition_by={
        "field": "dbt_updated_at",
        "data_type": "date",
        "granularity": "day"
      }
    )
  }}

  select
    external_ref,
    status,
    source,
    has_chargeback,
    current_timestamp as updated_at
  from {{ ref('int_chargeback_transactions') }}

{% endsnapshot %}
