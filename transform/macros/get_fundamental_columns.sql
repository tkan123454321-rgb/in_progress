{% macro get_fundamental_columns(fundamental_type) %}
    {% if fundamental_type == 'fundamental_1' %}
        {{ return([
            {'json_key': 'sharesOutstanding', 'type': 'DOUBLE', 'alias': 'shares_outstanding', 'is_mandatory': True},
            {'json_key': 'freeShares', 'type': 'DOUBLE', 'alias': 'floating_shares', 'is_mandatory': False},
            {'json_key': 'marketCap', 'type': 'DOUBLE', 'alias': 'market_cap', 'is_mandatory': True},
            {'json_key': 'avgVolume3m', 'type': 'DOUBLE', 'alias': 'avg_volume_3m', 'is_mandatory': True},
            {'json_key': 'insiderOwnership', 'type': 'DOUBLE', 'alias': 'insider_ownership', 'is_mandatory': False},
            {'json_key': 'institutionOwnership', 'type': 'DOUBLE', 'alias': 'institution_ownership', 'is_mandatory': False},
            {'json_key': 'foreignOwnership', 'type': 'DOUBLE', 'alias': 'foreign_ownership', 'is_mandatory': False}
        ]) }}
    {% elif fundamental_type == 'fundamental_2' %}
        {{ return([
            {'json_key': 'exchange', 'type': 'VARCHAR', 'alias': 'exchange', 'is_mandatory': True},
            {'json_key': 'isListing', 'type': 'BOOLEAN', 'alias': 'is_listing', 'is_mandatory': True}
        ]) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid fundamental type: " ~ fundamental_type) }}
    {% endif %}
{% endmacro %}