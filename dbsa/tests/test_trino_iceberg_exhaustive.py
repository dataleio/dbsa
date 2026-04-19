"""Exhaustive Trino Iceberg dialect tests."""

import unittest

from dbsa.trino_iceberg import ExternalTableProperties, Table as IcebergDialect

from dbsa.tests.sql_helpers import norm_ws
from dbsa.tests.tables import PrestoLikeExample, PrestoNoPartition


class TestTrinoIcebergPartitioning(unittest.TestCase):
    def test_partitioning_includes_defined_and_custom_partitions(self):
        d = IcebergDialect(
            PrestoLikeExample(schema="ice", ds="'1'", aggregation="'2'"),
            custom_partitions=["region", "tier"],
        )
        self.assertEqual(
            norm_ws(d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"ice\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioning = ARRAY[ 'ds', 'aggregation', 'region', 'tier' ], format = 'ORC' )",
        )

    def test_custom_partitions_none_same_as_only_table_partitions(self):
        d = IcebergDialect(
            PrestoLikeExample(schema="ice", ds="'1'", aggregation="'2'"),
            custom_partitions=None,
        )
        self.assertEqual(
            norm_ws(d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"ice\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioning = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_location_property_name_from_external_props(self):
        etp = ExternalTableProperties("s3://ice/w/", configs={"table_type": "ICEBERG"})
        d = IcebergDialect(
            PrestoLikeExample(schema="i", ds="'1'", aggregation="'2'"),
        )
        self.assertEqual(
            norm_ws(d.get_create_table(external_table_properties=etp)),
            "CREATE TABLE IF NOT EXISTS \"i\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioning = ARRAY[ 'ds', 'aggregation' ], format = 'ORC', "
            "location = 's3://ice/w/', table_type = 'ICEBERG' )",
        )

    def test_create_table_suffix_filter(self):
        d = IcebergDialect(
            PrestoNoPartition(schema="np"),
        )
        self.assertEqual(
            norm_ws(d.get_create_table(suffix="_i", filter_fn=lambda c: c.name == "id")),
            "CREATE TABLE IF NOT EXISTS \"np\".\"presto_no_partition_i\" ( \"id\" BIGINT COMMENT 'pk' ) "
            "COMMENT 'Managed table without partitions (Trino WITH has only table props).' "
            "WITH ( format = 'ORC' )",
        )


if __name__ == "__main__":
    unittest.main()
