from . import ExternalTableProperties as BaseExternalTableProperties
from .presto import Table as BaseTable
from jinja2 import Template
import inspect

class ExternalTableProperties(BaseExternalTableProperties):
    def get_properies(self):
        properties = [
            Template("external_location = '{{ location }}'").render(location=self.location)
        ]

        for k, v in self.configs.items():
            properties.append(Template("{{ k }} = '{{ v }}'").render(k=k, v=v))

        return properties


class Table(BaseTable):
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
