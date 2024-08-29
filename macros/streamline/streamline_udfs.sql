{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(json OBJECT) 
    returns ARRAY 
    api_integration = AWS_ECLIPSE_API_STG_V2 
    AS {% if target.database == 'ECLIPSE' -%}
        'https://m4liqml8ch.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api' /* TODO: update when prod is deployed */
    {% else %}
        'https://m4liqml8ch.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %}
{% endmacro %}