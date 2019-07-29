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

    def get_create_table(self, filter_fn=None, external_table=False, hdfs_path=None, tblformat=None, tblproperties=None):
        return Template("""
            CREATE {% if external_table %}EXTERNAL {% endif %}TABLE IF NOT EXISTS {{ t.full_table_name(quoted=True, with_prefix=True) }} (
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
            {%- for property in t.properties %}
            {{ property }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            {%- if external_table and hdfs_path %}
            LOCATION '{{ hdfs_path }}'
            {%- endif %}
            {%- if tblproperties %}
            TBLPROPERTIES({{ ','.join(tblproperties) }})
            {%- endif %}
        """).render(t=self.table, filter_fn=filter_fn, external_table=external_table, hdfs_path=hdfs_path, tblformat=tblformat, tblproperties=tblproperties, inspect=inspect)

    def get_drop_table(self):
        return Template("""
            DROP TABLE IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True) }} PURGE
        """).render(t=self.table)

    def get_truncate_table(self):
        return Template("""
            TRUNCATE TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }}
        """).render(t=self.table)

    def get_msck_table(self):
        return Template("""
            MSCK REPAIR TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }}
        """).render(t=self.table)

    def get_add_current_partition(self, hdfs_path=None, condition='', params=None, ignored_partitions=None):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }} ADD IF NOT EXISTS PARTITION(
                {{ condition }}
            ) {% if hdfs_path %}LOCATION '{{ hdfs_path }}'{% endif %}
        """).render(
            t=self.table, 
            hdfs_path=hdfs_path, 
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_delete_current_partition(self, condition='', params=None, ignored_partitions=None):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }} DROP IF EXISTS PARTITION(
                {{ condition }}
            ) PURGE
        """).render(
            t=self.table, 
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_insert_into_from_table(self, source_table_name, filter_fn=None):
        return self.get_insert_into_via_select(select=source_table_name, filter_fn=filter_fn, embed_select=False)

    def get_insert_into_via_select(self, select, filter_fn=None, embed_select=True):
        return Template("""
            INSERT INTO {{ t.full_table_name(quoted=True, with_prefix=True) }} 
            {%- if t.partitions %}
            PARTITION (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }} = {{ partition.value }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            (
              {%- for column in t.columns(include_partitions=False, filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            SELECT
              {%- for column_value in t.column_values(include_partitions=False, filter_fn=filter_fn) %}
              {{ column_value }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ select.strip() if not embed_select else '({}) vw'.format(select.strip()) }}
        """).render(t=self.table, filter_fn=filter_fn, select=select, embed_select=embed_select)

    def get_insert_overwrite_via_select(self, select):
        return Template("""
            INSERT OVERWRITE TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }} 
            {%- if t.partitions %}
            PARTITION (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }} = {{ partition.value }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            {{ select }}
        """).render(t=self.table, select=select)

    def get_drop_current_partition_view(self, suffix='_latest'):
        return Template("""
            DROP VIEW IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, suffix=suffix)

    def get_create_current_partition_view(self, suffix='_latest', condition='', ignored_partitions=None, params=None):
        return Template("""
            CREATE OR REPLACE VIEW {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} AS
            SELECT 
              {%- for column in t.columns() %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ t.full_table_name(quoted=True, with_prefix=True) }}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
        """).render(t=self.table, condition=self.table.get_current_partition_condition(condition, ignored_partitions), suffix=suffix) \
            .format(**self.table.get_current_partition_params(params))
            
