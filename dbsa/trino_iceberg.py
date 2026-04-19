from .trino import ExternalTableProperties as TrinoExternalTableProperties
from .trino import Table as TrinoTable
from jinja2 import Template


class ExternalTableProperties(TrinoExternalTableProperties):
    """Iceberg external table props using ``location`` instead of ``external_location``."""

    def __init__(self, location, configs=None):
        super().__init__(location, configs, location_property_name='location')


class Table(TrinoTable):
    """Trino Iceberg: ``partitioning`` property and optional extra partition names."""

    _partition_property_name = 'partitioning'

    def __init__(self, *args, custom_partitions=None, **kwargs):
        """``custom_partitions`` are appended to table-defined partition columns in ``WITH``."""
        super().__init__(*args, **kwargs)
        self._custom_partitions = custom_partitions

    @property
    def custom_partitions(self):
        """Extra partition field names for Iceberg ``partitioning``."""
        return self._custom_partitions

    def get_partition_property(self):
        """``partitioning = ARRAY[…]`` including table partitions and ``custom_partitions``."""
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
