from .trino import ExternalTableProperties as TrinoExternalTableProperties
from .trino import Table as TrinoTable
from jinja2 import Template


class ExternalTableProperties(TrinoExternalTableProperties):
    def __init__(self, location, configs=None):
        super().__init__(location, configs, location_property_name='location')

class Table(TrinoTable):
    _partition_property_name = 'partitioning'
    
    def __init__(self, *args, custom_partitions=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._custom_partitions = custom_partitions

    @property
    def custom_partitions(self):
        return self._custom_partitions

    def get_partition_property(self):
        all_partitions = []
        if self.table.partitions:
            all_partitions.extend([partition.name for partition in self.table.partitions])
        
        if self.custom_partitions:
            all_partitions.extend(self.custom_partitions)

        if all_partitions:
            return Template(
            """{{ partition_property }} = ARRAY[
                {%- for partition in partitions %}
                '{{ partition }}'{% if not loop.last %},{% endif %}
                {%- endfor %}
              ]""").render(partitions=all_partitions, partition_property=self._partition_property_name)
        else:
            return None
