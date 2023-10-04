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
        Integer: 'INT',
        Bigint: 'BIGINT',
        Real: 'FLOAT',
        Double: 'DOUBLE',
        Decimal: 'DECIMAL({{ precision }},{{ scale }})',
        Varchar: 'STRING{% if attrs.length %}({{ attrs.length }}){% endif %}',
        Char: 'CHAR({{ length }})',
        Varbinary: 'BINARY({{ length }})',
        JSON: 'STRING',
        Date: 'DATE',
        Timestamp: 'TIMESTAMP',
        Array: 'ARRAY<{{ data_type.column_type }}>',
        Map: 'MAP<{{ primitive_type.column_type }}, {{ data_type.column_type }}>',
        Row: "STRUCT<{% for c in columns %}{{ c.quoted_name }} : {{ c.column_type }}{% if not loop.last %}, {% endif%}{% endfor %}>",
        IPAddress: 'STRING',
    }
    _req_properties = {
        Decimal: {'precision', 'scale'},
        Char: {'length'},
        Varbinary: {'length'},
        Format: {'format'},
        Bucket: {'by', 'count'},
    }
    _property_types = {
        Format: 'STORED AS {{ format }}',
        Bucket: 'CLUSTERED BY ({{ by }}) INTO {{ count }} BUCKETS',
    }
    _how_to_quote_table = '`{}`'
    _how_to_quote_column = '`{}`'
    _column_setter = '{} {}'
    _sample_value_function = 'MAX({c})'

    def get_create_table(self, filter_fn=None, external_table=False, hdfs_path=None, tblformat=None, tblproperties=None, suffix=''):
        return Template("""
            CREATE {% if external_table %}EXTERNAL {% endif %}TABLE IF NOT EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=False) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.comment %} COMMENT '{{ column.comment|replace("'", "`") }}'{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- if inspect.getdoc(t) %}
            COMMENT '{{ inspect.getdoc(t)|replace("'", "`")|trim }}'
            {%- endif %}
            {%- if t.partitions %}
            PARTITIONED BY (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }} {{ partition.column_type }}{% if partition.comment %} COMMENT '{{ partition.comment|replace("'", "`") }}'{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            {%- if tblformat %}
            {{ tblformat }}
            {%- endif %}
            {%- for property in t.get_properties() %}
            {{ property }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            {%- if external_table and hdfs_path %}
            LOCATION '{{ hdfs_path }}'
            {%- endif %}
            {%- if tblproperties %}
            TBLPROPERTIES({{ ','.join(tblproperties) }})
            {%- endif %}
        """).render(t=self.table, filter_fn=filter_fn, external_table=external_table, hdfs_path=hdfs_path, tblformat=tblformat, tblproperties=tblproperties, inspect=inspect, suffix=suffix)

    def get_drop_table(self, suffix=''):
        return Template("""
            DROP TABLE IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} PURGE
        """).render(t=self.table, suffix=suffix)

    def get_truncate_table(self, suffix=''):
        return Template("""
            TRUNCATE TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_msck_table(self, suffix=''):
        return Template("""
            MSCK REPAIR TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_add_current_partition(self, hdfs_path=None, condition='', params=None, ignored_partitions=None, suffix=''):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} ADD IF NOT EXISTS PARTITION(
              {{ condition }}
            ) {% if hdfs_path %}LOCATION '{{ hdfs_path }}'{% endif %}
        """).render(
            t=self.table,
            suffix=suffix,
            hdfs_path=hdfs_path,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_delete_current_partition(self, condition='', params=None, ignored_partitions=None, suffix=''):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix='') }} DROP IF EXISTS PARTITION(
              {{ condition }}
            ) PURGE
        """).render(
            t=self.table,
            suffix=suffix,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_select(self, filter_fn=None, suffix='', condition='', transforms=None, limit=None):
        return Template("""
            SELECT
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {% if tf[column.name] %}{{ tf[column.name].format(c=column.quoted_name) }} AS {{ column.quoted_name }}{% else %}{{ column.quoted_name }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
            {%- if limit %}
            LIMIT {{ limit }}
            {%- endif %}
        """).render(t=self.table, limit=limit, filter_fn=filter_fn, suffix=suffix, condition=condition, tf=transforms or {})

    def get_insert_into_from_table(self, source_table_name, filter_fn=None, suffix=''):
        return self.get_insert_into_via_select(select=source_table_name, filter_fn=filter_fn, embed_select=False, suffix=suffix)

    def get_insert_into_via_select(self, select, filter_fn=None, embed_select=True, suffix=''):
        ignore_const_partitions_fn = lambda x: (x.partition and not x.value) or not x.partition
        if filter_fn:
            combined_fn = lambda x: filter_fn(x) and ignore_const_partitions_fn(x)
        else:
            combined_fn = ignore_const_partitions_fn

        return Template("""
            INSERT INTO {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if t.partitions %}
            PARTITION (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }}{% if partition.value %} = {{ partition.value }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            (
              {%- for column in t.columns(include_partitions=True, filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            SELECT
              {%- for column_value in t.column_values(include_partitions=True, filter_fn=filter_fn) %}
              {{ column_value }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ select.strip() if not embed_select else '({}) vw'.format(select.strip()) }}
        """).render(t=self.table, filter_fn=combined_fn, select=select, embed_select=embed_select, suffix=suffix)

    def get_insert_overwrite_via_select(self, select, suffix=''):
        return Template("""
            INSERT OVERWRITE TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if t.partitions %}
            PARTITION (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }}{% if partition.value %} = {{ partition.value }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            {{ select }}
        """).render(t=self.table, select=select, suffix=suffix)

    def get_drop_current_partition_view(self, suffix='_latest'):
        return Template("""
            DROP VIEW IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_create_current_partition_view(self, suffix='_latest', condition='', ignored_partitions=None, params=None, transforms=None):
        return Template("""
            CREATE OR REPLACE VIEW {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} AS
            {{ select }}
        """).render(
            t=self.table,
            select=self.get_select_current_partition(condition=condition, ignored_partitions=ignored_partitions, params=params, transforms=transforms),
            suffix=suffix,
        )
