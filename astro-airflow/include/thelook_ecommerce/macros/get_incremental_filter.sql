{% macro get_incremental_filter(source_partition_date, target_partition_date=none, lookback_days=none, floor_date=none, floor_interval=none) %}

    {% if floor_date and floor_interval %}
        {{ exceptions.raise_compiler_error("get_incremental_filter: pass either floor_date or floor_interval, not both.") }}
    {% endif %}

    {# if target partition column differs from source (e.g. event_created_at → session_started_at), use it for the {{ this }} lookback #}
    {% set _target = target_partition_date if target_partition_date is not none else source_partition_date %}

    {# resolve lookback: explicit arg → dbt var → macro default #}
    {% set lookback_days = lookback_days if lookback_days is not none else var('lookback_days', 3) %}
    {% set start_date = var('reprocess_start_date', none) %}
    {% set end_date   = var('reprocess_end_date',   none) %}

    {# ── 1. Manual reprocess window (passed via --vars at runtime) ── #}
    {% if start_date and end_date %}
        and {{ source_partition_date }} >= '{{ start_date }}'
        and {{ source_partition_date }} <  '{{ end_date }}'

    {# ── 2. Standard incremental — max date minus lookback buffer ── #}
    {% elif is_incremental() %}
        and {{ source_partition_date }} >= (
            select date_sub(max({{ _target }}), interval {{ lookback_days }} day)
            from {{ this }}
        )

    {# ── 3. Full refresh floor — fixed date or relative interval ── #}
    {% elif floor_date %}
        and {{ source_partition_date }} >= '{{ floor_date }}'

    {% elif floor_interval %}
        and {{ source_partition_date }} >= date_sub(current_date(), interval {{ floor_interval }})

    {% endif %}

{% endmacro %}