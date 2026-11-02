{% macro classify_amount(amount_column) %}
    CASE
        WHEN {{ amount_column }} < 10 THEN 'micro'
        WHEN {{ amount_column }} >= 10 AND {{ amount_column }} < 50 THEN 'low'
        WHEN {{ amount_column }} >= 50 AND {{ amount_column }} < 200 THEN 'medium'
        WHEN {{ amount_column }} >= 200 AND {{ amount_column }} < 1000 THEN 'high'
        WHEN {{ amount_column }} >= 1000 THEN 'critical'
        ELSE 'unknown'
    END
{%- endmacro -%}

