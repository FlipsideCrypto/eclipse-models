{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(json OBJECT) 
    returns ARRAY 
    {% if target.database == 'ECLIPSE' -%}
    api_integration = AWS_ECLIPSE_API_PROD
    AS
        'https://8zxk0vgb25.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
    api_integration = AWS_ECLIPSE_API_STG_V2 
    AS
        'https://m4liqml8ch.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %}
{% endmacro %}