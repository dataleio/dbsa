"""Exhaustive Trino dialect SQL (and Trino-only helpers)."""

import datetime
import decimal
import unittest

from dbsa.trino import ExternalTableProperties, Table as TrinoDialect

from dbsa.tests.sql_helpers import norm_ws, one_line
from dbsa.tests.tables import PrestoLikeExample, PrestoNoPartition


class TestTrinoGetCreateTable(unittest.TestCase):
    def setUp(self):
        self.base = PrestoLikeExample(
            schema="analytics",
            ds="'2020-01-01'",
            aggregation="'hourly'",
        )

    def test_without_external_properties(self):
        self.assertEqual(
            norm_ws(TrinoDialect(self.base).get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"analytics\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_with_external_table_properties_mixed_config_types(self):
        etp = ExternalTableProperties(
            location="s3://b/p/",
            configs={"num_prop": 42, "str_prop": "v", "boolish": True},
        )
        self.assertEqual(
            norm_ws(TrinoDialect(self.base).get_create_table(external_table_properties=etp)),
            "CREATE TABLE IF NOT EXISTS \"analytics\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC', "
            "external_location = 's3://b/p/', num_prop = 42, str_prop = 'v', boolish = True )",
        )

    def test_create_table_suffix_and_filter(self):
        d = TrinoDialect(self.base)
        self.assertEqual(
            norm_ws(d.get_create_table(suffix="_t", filter_fn=lambda c: c.name != "value")),
            "CREATE TABLE IF NOT EXISTS \"analytics\".\"presto_like_example_t\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"grouping_id\" BIGINT COMMENT 'Grouping id', "
            "\"ds\" VARCHAR COMMENT 'Date key', \"aggregation\" VARCHAR COMMENT 'Aggregation name' ) "
            "COMMENT 'Example analytics table.' WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], "
            "format = 'ORC' )",
        )


class TestTrinoGetPartitionProperty(unittest.TestCase):
    def test_partitioned_by_array(self):
        d = TrinoDialect(
            PrestoLikeExample(schema="s", ds="'1'", aggregation="'2'"),
        )
        self.assertEqual(
            norm_ws(d.get_partition_property()),
            "partitioned_by = ARRAY[ 'ds', 'aggregation' ]",
        )

    def test_no_partitions_returns_none(self):
        d = TrinoDialect(PrestoNoPartition(schema="s"))
        self.assertIsNone(d.get_partition_property())


class TestTrinoGetCreateTableProperties(unittest.TestCase):
    def test_order_partition_then_format_then_external(self):
        d = TrinoDialect(
            PrestoLikeExample(schema="s", ds="'1'", aggregation="'2'"),
        )
        props = d.get_create_table_properties(
            external_table_properties=ExternalTableProperties("loc"),
        )
        self.assertEqual(
            [norm_ws(p) for p in props],
            [
                "partitioned_by = ARRAY[ 'ds', 'aggregation' ]",
                "format = 'ORC'",
                "external_location = 'loc'",
            ],
        )


class TestTrinoGetCurrentPartitionList(unittest.TestCase):
    def test_both_partitions_and_ignore(self):
        d = TrinoDialect(
            PrestoLikeExample(schema="s", ds="'1'", aggregation="'2'"),
        )
        self.assertEqual(
            d.get_current_partition_list(ignored_partitions=["aggregation"]),
            "ARRAY['ds'], ARRAY[{ds}]",
        )


class TestTrinoGetAddCurrentPartition(unittest.TestCase):
    def setUp(self):
        self.d = TrinoDialect(
            PrestoLikeExample(schema="st", ds="'1'", aggregation="'2'"),
        )

    def test_create_empty_partition_without_hdfs(self):
        self.assertEqual(
            one_line(self.d.get_add_current_partition()),
            "CALL system.create_empty_partition('st', 'presto_like_example', ARRAY['ds', 'aggregation'], ARRAY['1', '2'])",
        )

    def test_register_partition_with_path(self):
        self.assertEqual(
            one_line(self.d.get_add_current_partition(hdfs_path="/p")),
            "CALL system.register_partition('st', 'presto_like_example', ARRAY['ds', 'aggregation'], ARRAY['1', '2'], '/p')",
        )

    def test_numeric_and_datetime_params_quoted(self):
        d = TrinoDialect(
            PrestoLikeExample(schema="st", ds=1, aggregation=decimal.Decimal("2.5")),
        )
        self.assertEqual(
            one_line(
                d.get_add_current_partition(
                    ignored_partitions=["aggregation"],
                    params={"ds": datetime.date(2024, 1, 15)},
                )
            ),
            "CALL system.create_empty_partition('st', 'presto_like_example', ARRAY['ds'], ARRAY['2024-01-15'])",
        )


class TestTrinoInheritsPrestoSql(unittest.TestCase):
    """Trino dialect delegates most generators to Presto implementation."""

    def setUp(self):
        self.d = TrinoDialect(
            PrestoLikeExample(
                schema="analytics",
                ds="'2020-01-01'",
                aggregation="'hourly'",
            )
        )

    def test_delete_from_and_truncate(self):
        self.assertEqual(
            norm_ws(self.d.get_delete_from()),
            'DELETE FROM "analytics"."presto_like_example"',
        )
        self.assertEqual(
            one_line(self.d.get_truncate_table()),
            'TRUNCATE TABLE "analytics"."presto_like_example"',
        )


if __name__ == "__main__":
    unittest.main()
