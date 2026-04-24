{% macro get_dividend_columns() %}
    {{ return([
        {'json_key': 'cashDividend', 'type': 'DOUBLE', 'alias': 'cash_dividend', 'is_mandatory': False, 'must_be_positive': True},
        {'json_key': 'stockDividend', 'type': 'DOUBLE', 'alias': 'stock_dividend', 'is_mandatory': False, 'must_be_positive': True}
    ]) }}
{% endmacro %}