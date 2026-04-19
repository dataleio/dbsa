"""Smoke tests across dialects (detailed coverage lives in ``test_*_exhaustive``)."""

import unittest

from dbsa.hive import Table as HiveDialect
from dbsa.presto import Table as PrestoDialect
from dbsa.redshift import Table as RedshiftDialect
from dbsa.trino import Table as TrinoDialect
from dbsa.trino_iceberg import Table as TrinoIcebergDialect

from dbsa.tests.sql_helpers import norm_ws
from dbsa.tests.tables import HiveLikeExample, PrestoLikeExample, RedshiftLikeExample


class TestPrestoDialect(unittest.TestCase):
    def setUp(self):
        self.base = PrestoLikeExample(
            schema="default",
            ds="'2019-07-27'",
            aggregation="'daily'",
        )
        self.d = PrestoDialect(self.base)

    def test_create_table_contains_columns_and_with(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"default\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_delete_current_partition(self):
        self.assertEqual(
            norm_ws(self.d.get_delete_current_partition(ignored_partitions=["aggregation"])),
            'DELETE FROM "default"."presto_like_example" WHERE "ds" = \'2019-07-27\'',
        )

    def test_select_current_partition(self):
        self.assertEqual(
            norm_ws(self.d.get_select_current_partition(ignored_partitions=["aggregation"])),
            "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27'",
        )

    def test_truncate_and_drop_table(self):
        self.assertEqual(
            norm_ws(self.d.get_truncate_table()),
            'TRUNCATE TABLE "default"."presto_like_example"',
        )
        self.assertEqual(
            norm_ws(self.d.get_drop_table()),
            'DROP TABLE IF EXISTS "default"."presto_like_example"',
        )

    def test_insert_into_via_select(self):
        self.assertEqual(
            norm_ws(self.d.get_insert_into_via_select("SELECT 1", embed_select=True)),
            "INSERT INTO \"default\".\"presto_like_example\" ( \"ds\", \"aggregation\", "
            "\"metric\", \"value\", \"grouping_id\" ) SELECT '2019-07-27' AS \"ds\", "
            "'daily' AS \"aggregation\", \"metric\", \"value\", \"grouping_id\" FROM (SELECT 1) AS vw",
        )


class TestTrinoDialect(unittest.TestCase):
    def setUp(self):
        self.d = TrinoDialect(
            PrestoLikeExample(
                schema="analytics",
                ds="'2020-01-01'",
                aggregation="'hourly'",
            )
        )

    def test_create_table_uses_trino_with_block(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"analytics\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )


class TestTrinoIcebergDialect(unittest.TestCase):
    def setUp(self):
        self.d = TrinoIcebergDialect(
            PrestoLikeExample(
                schema="ice",
                ds="'2020-01-01'",
                aggregation="'hourly'",
            ),
            custom_partitions=["region"],
        )

    def test_create_table_uses_partitioning_property(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"ice\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioning = ARRAY[ 'ds', 'aggregation', 'region' ], format = 'ORC' )",
        )


class TestHiveDialect(unittest.TestCase):
    def setUp(self):
        self.d = HiveDialect(
            HiveLikeExample(schema="dw", ds="'2021-06-01'"),
        )

    def test_create_table_partitioned_by_clause(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS `dw`.`hive_like_example` ( `metric` STRING COMMENT 'Metric name' ) "
            "COMMENT 'Hive example.' PARTITIONED BY ( `ds` STRING COMMENT 'Date key' ) STORED AS ORC",
        )

    def test_delete_current_partition(self):
        self.assertEqual(
            norm_ws(self.d.get_delete_current_partition()),
            "ALTER TABLE `dw`.`hive_like_example` DROP IF EXISTS PARTITION( `ds` = '2021-06-01' ) PURGE",
        )

    def test_insert_into_via_select(self):
        self.assertEqual(
            norm_ws(
                self.d.get_insert_into_via_select(
                    "SELECT `metric` FROM src", embed_select=True
                )
            ),
            "INSERT INTO `dw`.`hive_like_example` PARTITION ( `ds` = '2021-06-01' ) ( `metric` ) "
            "SELECT `metric` FROM (SELECT `metric` FROM src) vw",
        )


class TestRedshiftDialect(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftLikeExample(schema="public"))

    def test_create_table_sortkey_and_encode(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example" ( "ds" VARCHAR(32) ENCODE ZSTD, '
            '"metric" VARCHAR(256) ENCODE ZSTD, "n" BIGINT ENCODE RAW ) SORTKEY("ds", "metric");',
        )

    def test_truncate_table(self):
        self.assertEqual(
            norm_ws(self.d.get_truncate_table()),
            'TRUNCATE TABLE "public"."redshift_like_example";',
        )


if __name__ == "__main__":
    unittest.main()
