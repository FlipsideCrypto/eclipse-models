{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {% if target.database != "ECLIPSE_COMMUNITY_DEV" %}
            {{ create_udf_bulk_rest_api_v2() }};
        {% endif %}

        {{ create_udf_ordered_signers(schema = "silver") }}
        {{ create_udf_get_compute_units_consumed(schema = "silver") }}
        {{ create_udf_get_compute_units_total(schema = "silver") }}
        {{ create_udf_get_tx_size(schema = "silver") }}
        {{ create_udf_get_all_inner_instruction_program_ids(schema = "silver") }}
        {{ create_udf_get_logs_program_data(schema = "silver") }}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
