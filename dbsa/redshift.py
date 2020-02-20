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
              {{ column.quoted_name }} {{ column.column_type }}{% if column.default_value %} DEFAULT {{ column.default_value }}{% endif %}{% if column.encode %} ENCODE {{ column.encode|upper }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- for property in t.properties %}
            {{ property }}
            {%- endfor %};
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix)

    def get_create_external_table(self, hdfs_path, fileformat, tblformat, tblproperties=None, filter_fn=None, suffix=''):
        return Template("""
            CREATE EXTERNAL TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=False) %}
              {{ column.quoted_name }} {{ column.column_type }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- if t.partitions %}
            PARTITIONED BY (
              {%- for partition in t.partitions %}
              {{ partition.quoted_name }} {{ partition.column_type }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
            {{ tblformat }}
            STORED AS {{ fileformat }}
            LOCATION '{{ hdfs_path }}'
            {%- if tblproperties %}
            TABLE PROPERTIES ({{ ','.join(tblproperties) }})
            {%- endif %}
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix, tblformat=tblformat, fileformat=fileformat, tblproperties=tblproperties, hdfs_path=hdfs_path)

    def get_create_staging_table(self, cleanup_fn=cleanup_fn, filter_fn=None, include_partitions=False, suffix=''):
        return Template("""
            CREATE TABLE IF NOT EXISTS {{ t.full_staging_table_name(cleanup_fn=cleanup_fn, quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=include_partitions) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.default_value %} DEFAULT {{ column.default_value }}{% endif %}{% if column.encode %} ENCODE {{ column.encode|upper }}{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            );
        """).render(t=self.table, cleanup_fn=cleanup_fn, filter_fn=filter_fn, include_partitions=include_partitions, suffix=suffix)

    def get_add_external_current_partition(self, hdfs_path=None, condition='', params=None, ignored_partitions=None, suffix=''):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} ADD IF NOT EXISTS PARTITION(
              {{ condition }}
            ) LOCATION '{{ hdfs_path }}'
        """).render(
            t=self.table,
            suffix=suffix,
            hdfs_path=hdfs_path,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_delete_external_current_partition(self, condition='', params=None, ignored_partitions=None, suffix=''):
        return Template("""
            ALTER TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix='') }} DROP IF EXISTS PARTITION(
              {{ condition }}
            )
        """).render(
            t=self.table,
            suffix=suffix,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions, sep=', ') \
                .format(**self.table.get_current_partition_params(params))
        )

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
            TRUNCATE TABLE {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }};
        """).render(t=self.table, suffix=suffix)

    def get_update_current_partition_for_manually_set_columns(self, suffix='', condition='', ignored_partitions=None, params=None):
        filter_fn = lambda x: x.manually_set
        if not len(self.table.columns(filter_fn=filter_fn, include_partitions=False)):
            return ''

        return Template("""
            UPDATE {{ t.full_table_name(quoted=True, with_prefix=True) }}
            SET
            {%- for column in t.columns(filter_fn=filter_fn, include_partitions=False) %}
              {{ column.quoted_name }} = {{ column.value }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
        """).render(t=self.table, suffix=suffix, filter_fn=filter_fn,
                    condition=self.table.get_current_partition_condition(condition, ignored_partitions) \
                        .format(**self.table.get_current_partition_params(params)))

    def get_copy_to_staging(self, cleanup_fn=cleanup_fn, filter_fn=None, include_partitions=False, suffix=''):
        return Template("""
            COPY {{ t.full_staging_table_name(cleanup_fn=cleanup_fn, quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in t.columns(filter_fn=filter_fn, include_partitions=include_partitions) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {% raw %}
            FROM '{{ '{{ path_prefix }}://{{ path }}' }}'
            {{ '{% if access_key and secret_key %}' }}
            WITH CREDENTIALS '{{ 'aws_access_key_id={{ access_key }};aws_secret_access_key={{ secret_key }}' }}'
            {{ '{% else %}' }}
            IAM_ROLE '{{ '{{ iam_role }}' }}'
            {{ '{% endif %}' }}
            {{ '{{ copy_options }}' }}
            {% endraw %};
        """).render(t=self.table, cleanup_fn=cleanup_fn, filter_fn=filter_fn, include_partitions=include_partitions, suffix=suffix)

    def get_select(self, filter_fn=None, suffix='', condition='', order_by_sortkey=False, use_star=False):
        sortkey = self.table.get_property_by_type(Sortkey) \
            if order_by_sortkey \
            else None

        return Template("""
            SELECT
              {%- if use_star %}
              *
              {%- else %}
              {%- for column in t.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }}{% if not loop.last %},{% endif %}
              {%- endfor %}
              {%- endif %}
            FROM {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }}
            {%- if condition %}
            WHERE {{ condition }}
            {%- endif %}
            {%- if sortkey %}
            ORDER BY {% for c in sortkey.attrs['keys'] %}"{{ c }}"{% if not loop.last %}, {% endif %}{% endfor %}
            {%- endif %}
        """).render(t=self.table, filter_fn=filter_fn, suffix=suffix, condition=condition, sortkey=sortkey, use_star=use_star)

    def get_unload_table(self, filter_fn=None):
        return self.get_unload_via_select(select=self.get_select(filter_fn))

    @classmethod
    def get_unload_via_select(cls, select):
        return Template(Template("""
            UNLOAD ('
              {{ select }}
            ')
            TO '{{ 's3://{{ s3_bucket }}/{{ s3_key }}' }}'
            {{ '{% if access_key and secret_key %}' }}
            WITH CREDENTIALS '{{ 'aws_access_key_id={{ access_key }};aws_secret_access_key={{ secret_key }}' }}'
            {{ '{% else %}' }}
            IAM_ROLE '{{ '{{ iam_role }}' }}'
            {{ '{% endif %}' }}
            {{ '{{ unload_options }}' }};
        """).render(select=select.strip().strip(';').translate(str.maketrans({"'": r"\'"}))))

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
            {{ select }};
        """).render(
            t=self.table,
            select=self.get_select_current_partition(condition=condition, ignored_partitions=ignored_partitions, params=params),
            suffix=suffix,
        )
