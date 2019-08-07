from . import (
    Boolean,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Real,
    Double,
    Decimal,
    Varchar,
    Char,
    Varbinary,
    JSON,
    Date,
    Time,
    Timestamp,
    Array,
    Map,
    Row,
    IPAddress,
    Format,
    Bucket,
    Dialect as BaseDialect,
)
from jinja2 import Template
import inspect

class Table(BaseDialect):
    _column_types = {
        Boolean: 'BOOLEAN',
        Tinyint: 'TINYINT',
        Smallint: 'SMALLINT',
        Integer: 'INTEGER',
        Bigint: 'BIGINT',
        Real: 'REAL',
        Double: 'DOUBLE',
        Decimal: 'DECIMAL({{ precision }},{{ scale }})',
        Varchar: 'VARCHAR{% if attrs.length %}({{ attrs.length }}){% endif %}',
        Char: 'CHAR({{ length }})',
        Varbinary: 'VARBINARY({{ length }})',
        JSON: 'JSON',
        Date: 'DATE',
        Time: 'TIME',
        Timestamp: 'TIMESTAMP',
        Array: 'ARRAY({{ data_type.column_type }})',
        Map: 'MAP({{ primitive_type.column_type }}, {{ data_type.column_type }})',
        Row: "ROW({% for c in columns %}{{ c.quoted_name }} {{ c.column_type }}{% if not loop.last %}, {% endif%}{% endfor %})",
        IPAddress: 'IPADDRESS',
    }
    _req_properties = {
        Decimal: {'precision', 'scale'},
        Char: {'length'},
        Varbinary: {'length'},
        Format: {'format'},
        Bucket: {'by', 'count'},
    }
    _property_types = {
        Format: "format = '{{ format }}'",
        Bucket: 'bucketed_by = "{{ by }}", bucket_count = {{ count }}'
    }
    _how_to_quote_table = '"{}"'
    _how_to_quote_column = '"{}"'
    _column_setter = '{} AS {}'

    def columns(self, include_partitions=True, filter_fn=None):
        columns = self.table._columns if not filter_fn else filter(filter_fn, self.table._columns)
        kept_partitions = []
        for c in columns:
            if not c.partition:
                yield c
            else:
                kept_partitions.append(c)

        if include_partitions:
            for c in kept_partitions:
                yield c

    def get_create_table(self, filter_fn=None, suffix=''):
        return Template("""
            CREATE TABLE IF NOT EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in d.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.comment %} COMMENT '{{ column.comment|replace("'", "''") }}'{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- if inspect.getdoc(t) %}
            COMMENT '{{ inspect.getdoc(t)|replace("'", "''")|trim }}'
            {%- endif %}
            {%- if t.properties or t.partitions %}
            WITH (
              {%- if t.partitions %}
              partitioned_by = ARRAY[
                {%- for partition in t.partitions %}
                '{{ partition.name }}'{% if not loop.last %},{% endif %}
                {%- endfor %}
              ]{% if t.properties %},{% endif %}
              {%- endif %}
              {%- for property in t.properties %}
              {{ property }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
        """).render(t=self.table, d=self, filter_fn=filter_fn, inspect=inspect, suffix=suffix)

    def get_drop_table(self, suffix=''):
        return Template("""
            DROP TABLE IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_truncate_table(self, suffix=''):
        return Template("""
            TRUNCATE TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_delete_from(self, condition=None, params=None, suffix=''):
        return Template("""
            DELETE FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
        """).render(t=self.table, suffix=suffix, condition=condition).format(**(params or {}))

    def get_select(self, filter_fn=None, suffix='', condition=''):
        return Template("""
            SELECT
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix, condition=condition)

    def get_insert_into_from_table(self, source_table_name, filter_fn=None, suffix=''):
        return self.get_insert_into_via_select(select=source_table_name, filter_fn=filter_fn, embed_select=False, suffix=suffix)

    def get_insert_into_via_select(self, select, filter_fn=None, embed_select=True, suffix=''):
        return Template("""
            INSERT INTO {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            SELECT
              {%- for column_value in t.column_values(filter_fn=filter_fn) %}
              {{ column_value }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ select if not embed_select else '({}) AS vw'.format(select) }}
        """).render(t=self.table, select=select, embed_select=embed_select, suffix=suffix)

    def get_drop_current_partition_view(self, suffix='_latest'):
        return Template("""
            DROP VIEW IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_create_current_partition_view(self, suffix='_latest', condition='', ignored_partitions=None, params=None):
        return Template("""
            CREATE OR REPLACE VIEW {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} AS
            {{ select }}
        """).render(
            t=self.table,
            select=self.get_select_current_partition(condition=condition, ignored_partitions=ignored_partitions, params=params),
            suffix=suffix,
        )
