{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    -- Adapted for BigQuery from the Snowflake source
    CAST((-1 * {{extended_price}} * {{discount_percentage}}) AS NUMERIC)
{% endmacro %}
