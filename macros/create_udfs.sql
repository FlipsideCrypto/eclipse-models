{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {% if target.database != "ECLIPSE_COMMUNITY_DEV" %}
            {{ create_udf_bulk_rest_api_v2() }};
        {% endif %}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
