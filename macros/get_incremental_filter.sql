{% macro get_incremental_filter(partition_expr, lookback_days=none, floor_date=none, floor_interval=none) %}

    {% if floor_date and floor_interval %}
        {{ exceptions.raise_compiler_error("get_incremental_filter: pass either floor_date or floor_interval, not both.") }}
    {% endif %}

    {# resolve lookback: explicit arg → dbt var → macro default #}
    {% set lookback_days = lookback_days if lookback_days is not none else var('lookback_days', 3) %}
    {% set start_date = var('reprocess_start_date', none) %}
    {% set end_date   = var('reprocess_end_date',   none) %}

    {# ── 1. Manual reprocess window (passed via --vars at runtime) ── #}
    {% if start_date and end_date %}
        and {{ partition_expr }} >= '{{ start_date }}'
        and {{ partition_expr }} <  '{{ end_date }}'

    {# ── 2. Standard incremental — max date minus lookback buffer ── #}
    {% elif is_incremental() %}
        and {{ partition_expr }} >= (
            select date_sub(max({{ partition_expr }}), interval {{ lookback_days }} day)
            from {{ this }}
        )

    {# ── 3. Full refresh floor — fixed date or relative interval ── #}
    {% elif floor_date %}
        and {{ partition_expr }} >= '{{ floor_date }}'

    {% elif floor_interval %}
        and {{ partition_expr }} >= date_sub(current_date(), interval {{ floor_interval }})

    {% endif %}

{% endmacro %}