from . import ExternalTableProperties as BaseExternalTableProperties
from .presto import Table as BaseTable
from jinja2 import Template
import inspect
import datetime
import numbers
import decimal

class ExternalTableProperties(BaseExternalTableProperties):
    def get_properies(self):
        properties = [
            Template("external_location = '{{ location }}'").render(location=self.location)
        ]

        for k, v in self.configs.items():
            properties.append(Template("{{ k }} = '{{ v }}'").render(k=k, v=v))

        return properties


class Table(BaseTable):
    _how_to_quote_string = "'{}'"

    def get_create_table_properties(self, external_table_properties=None):
        create_table_properties = []

        if self.table.partitions:
            create_table_properties.append(Template(
            """partitioned_by = ARRAY[
                {%- for partition in t.partitions %}
                '{{ partition.name }}'{% if not loop.last %},{% endif %}
                {%- endfor %}
              ]""").render(t=self.table))

        if self.table.get_properties():
            create_table_properties.append(Template(
            """{%- for property in t.get_properties() %}
              {{ property }}{% if not loop.last %},{% endif %}
              {%- endfor %}""").render(t=self.table))

        if external_table_properties and external_table_properties.get_properies():
            create_table_properties.append(Template(
            """{%- for property in etp %}
              {{ property }}{% if not loop.last %},{% endif %}
              {%- endfor %}""").render(etp=external_table_properties.get_properies()))

        return create_table_properties

    def get_create_table(self, filter_fn=None, suffix='', external_table_properties=None):
        return Template("""
            CREATE TABLE IF NOT EXISTS {{ t.full_table_name(quoted=True, with_prefix=True, suffix=suffix) }} (
              {%- for column in d.columns(filter_fn=filter_fn) %}
              {{ column.quoted_name }} {{ column.column_type}}{% if column.comment %} COMMENT '{{ column.comment|replace("'", "''") }}'{% endif %}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- if inspect.getdoc(t) %}
            COMMENT '{{ inspect.getdoc(t)|replace("'", "''")|trim }}'
            {%- endif %}
            {%- if tbl_properties %}
            WITH (
              {%- for property in tbl_properties %}
              {{ property }}{% if not loop.last %},{% endif %}
              {%- endfor %}
            )
            {%- endif %}
        """).render(t=self.table, d=self, filter_fn=filter_fn, inspect=inspect, suffix=suffix, tbl_properties=self.get_create_table_properties(external_table_properties))

    def get_current_partition_list(self, ignored_partitions=None):
        partition_names = {p.name for p in self.partitions} - set(ignored_partitions or [])
        partitions = [p for p in self.partitions if p.name in partition_names]
        partition_list = ', '.join([f"'{p.name}'" for p in partitions])
        partition_values = ', '.join([f"{{{p.name}}}" for p in partitions])

        return f'[{partition_list}], [{partition_values}]'

    def _param_to_quoted_sting(self, param):
        if isinstance(param, (int, float, numbers.Number, decimal.Decimal)):
            return self._how_to_quote_string.format(str(param))
        if isinstance(param, (datetime.date, datetime.datetime)):
            return self._how_to_quote_string.format(param.isoformat())
        return param

    def get_add_current_partition(self, hdfs_path=None, condition='', params=None, ignored_partitions=None, suffix=''):
        current_partition_params = {k: self._param_to_quoted_sting(v) for k, v in self.table.get_current_partition_params(params).items()}

        return Template("""
            CALL system.{% if hdfs_path %}register_partition{% else %}create_empty_partition{% endif %}('{{ t.schema }}', '{{ t.table_name_with_prefix }}', {{ condition }}{% if hdfs_path %}, '{{ hdfs_path }}'{% endif %})
        """).render(
            t=self.table,
            suffix=suffix,
            hdfs_path=hdfs_path,
            condition=self.get_current_partition_list(ignored_partitions) \
                .format(**current_partition_params)
        )
