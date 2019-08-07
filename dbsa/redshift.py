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
    Date,
    Timestamp,
    Sortkey,
    DistributionKey,
    cleanup_fn,
    Dialect as BaseDialect,
)
from jinja2 import Template
import json

COLUMN_ENCODE = ['BYTEDICT', 'DELTA', 'DELTA32K', 'LZO', 'MOSTLY8', 'MOSTLY16', 'MOSTLY32', 'RAW', 'RUNLENGTH', 'TEXT255', 'TEXT32K', 'ZSTD']

class Table(BaseDialect):
    _column_types = {
        Boolean: 'BOOLEAN',
        Tinyint: 'TINYINT',
        Smallint: 'SMALLINT',
        Integer: 'INTEGER',
        Bigint: 'BIGINT',
        Real: 'REAL',
        Double: 'FLOAT',
        Decimal: 'NUMERIC({{ precision }},{{ scale }})',
        Varchar: 'VARCHAR({{ length }})',
        Char: 'CHAR({{ length }})',
        Date: 'DATE',
        Timestamp: 'TIMESTAMP',
    }
    _req_properties = {
        Tinyint: {'encode'},
        Smallint: {'encode'},
        Integer: {'encode'},
        Bigint: {'encode'},
        Real: {'encode'},
        Double: {'encode'},
        Decimal: {'precision', 'scale', 'encode'},
        Char: {'length', 'encode'},
        Varchar: {'length', 'encode'},
        Date: {'encode'},
        Timestamp: {'encode'},
        Sortkey: {'keys'},
        DistributionKey: {'key'},
    }
    _property_types = {
        Sortkey: 'SORTKEY({% for c in keys %}"{{ c }}"{% if not loop.last %}, {% endif%}{% endfor %})',
        DistributionKey: 'DISTKEY("{{ key }}")',
    }
    _how_to_quote_table = '"{}"'
    _how_to_quote_column = '"{}"'
    _column_setter = '{} AS {}'

    ENCODE=dict(zip(COLUMN_ENCODE, COLUMN_ENCODE))

    @property
    def jsonpath(self):
        return json.dumps({
            'jsonpaths': [
                c.attrs['jsonpath']
                for c in self.table.columns()
                if 'jsonpath' in c.attrs
            ]
        })

    def get_create_table(self, filter_fn=None, suffix=''):
        return Template("""
            CREATE TABLE IF NOT EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.encode %} ENCODE {{ column.encode|upper }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- for property in t.properties %}
            {{ property }}
            {%- endfor %};
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix)

    def get_create_staging_table(self, cleanup_fn=cleanup_fn, filter_fn=None, include_partitions=False, suffix=''):
        return Template("""
            CREATE TABLE IF NOT EXISTS {{ t.full_staging_table_name(cleanup_fn=cleanup_fn, quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=include_partitions) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.encode %} ENCODE {{ column.encode|upper }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            );
        """).render(t=self.table, cleanup_fn=cleanup_fn, filter_fn=filter_fn, include_partitions=include_partitions, suffix=suffix)

    def get_drop_table(self, suffix=''):
        return Template("""
            DROP TABLE IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }};
        """).render(t=self.table, suffix=suffix)

    def get_drop_staging_table(self, suffix=''):
        return Template("""
            DROP TABLE IF EXISTS {{ t.full_staging_table_name(quoted=True, with_prefix=True, suffix=suffix) }};
        """).render(t=self.table, suffix=suffix)

    def get_truncate_table(self, suffix=''):
        return Template("""
            TRUNCATE TABLE {{ t.full_table_name(quoted=True, with_prefix=True) }};
        """).render(t=self.table, suffix=suffix)

    def get_copy_to_staging(self, cleanup_fn=cleanup_fn, filter_fn=None, include_partitions=False, suffix=''):
        return Template("""
            COPY {{ t.full_staging_table_name(cleanup_fn=cleanup_fn, quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=include_partitions) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {% raw %}
            FROM '{{ 's3://{{ s3_bucket }}/{{ s3_key }}' }}'
            WITH CREDENTIALS '{{ 'aws_access_key_id={{ access_key }};aws_secret_access_key={{ secret_key }}' }}'
            {{ '{{ copy_options }}' }}
            {% endraw %};
        """).render(t=self.table, cleanup_fn=cleanup_fn, filter_fn=filter_fn, include_partitions=include_partitions, suffix=suffix)

    def get_select(self, filter_fn=None, suffix=''):
        return Template("""
            SELECT
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix)

    def get_unload_table(self, filter_fn=None):
        return self.get_unload_via_select(select=self.get_select(filter_fn))

    @classmethod
    def get_unload_via_select(cls, select):
        return Template(Template("""
            UNLOAD ('
              {{ select }}
            ')
            TO '{{ s3_path }}'
            WITH CREDENTIALS '{{ credentials }}'
            {{ unload_options }};
        """).render(
            select=select.strip().strip(';').translate(str.maketrans({"'": r"\'"})),
            s3_path='s3://{{ s3_bucket }}/{{ s3_key }}',
            credentials='aws_access_key_id={{ access_key }};aws_secret_access_key={{ secret_key }}',
            unload_options='{{ unload_options }}',
        ))

    def get_delete_from(self, condition=None, params=None, using=None, suffix=''):
        r = Template("""
            DELETE FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if using %}
            USING {{ using }} AS u
            {%- endif %}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %};
        """).render(t=self.table, condition=condition, using=using, suffix=suffix)
        if params:
            return r.format(**(params or {}))
        return r

    def get_delete_upsert(self, pk_columns, cleanup_fn=cleanup_fn, using=None, params=None, suffix=''):
        table = self.table.full_table_name(quoted=True, with_prefix=True, suffix=suffix)
        if not using:
            using = self.table.full_staging_table_name(cleanup_fn=cleanup_fn, quoted=True, with_prefix=True, suffix=suffix)

        condition = ' AND '.join(
            'u."{c}" = {table}."{c}"'.format(c=c, table=table)
            for c in pk_columns
        )
        return self.get_delete_from(condition, using=using, params=params, suffix=suffix)

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
            FROM {{ select if not embed_select else '({}) AS vw'.format(select.strip().strip(';')) }};
        """).render(t=self.table, select=select, embed_select=embed_select, filter_fn=filter_fn, suffix=suffix)

    def get_drop_current_partition_view(self, suffix='_latest'):
        return Template("""
            DROP VIEW IF EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }};
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
            {%- endif %};
        """).render(
            t=self.table,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions) \
                .format(**self.table.get_current_partition_params(params)), \
            suffix=suffix
        )
