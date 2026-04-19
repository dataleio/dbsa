"""Exhaustive Presto dialect SQL generation tests (methods and parameters)."""

import unittest

from dbsa.presto import Table as PrestoDialect

from dbsa.tests.sql_helpers import norm_ws, one_line
from dbsa.tests.tables import PrestoBareMin, PrestoBucketed, PrestoLikeExample


class TestPrestoGetCreateTable(unittest.TestCase):
    def setUp(self):
        self.t = PrestoLikeExample(
            schema="default",
            ds="'2019-07-27'",
            aggregation="'daily'",
        )
        self.d = PrestoDialect(self.t)

    def test_default(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"default\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_suffix(self):
        self.assertEqual(
            one_line(self.d.get_create_table(suffix="_stg")),
            "CREATE TABLE IF NOT EXISTS \"default\".\"presto_like_example_stg\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id', \"ds\" VARCHAR COMMENT 'Date key', "
            "\"aggregation\" VARCHAR COMMENT 'Aggregation name' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_filter_fn_excludes_partition_columns(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table(filter_fn=lambda c: not c.partition)),
            "CREATE TABLE IF NOT EXISTS \"default\".\"presto_like_example\" ( "
            "\"metric\" VARCHAR COMMENT 'Metric name', \"value\" DOUBLE COMMENT 'Metric value', "
            "\"grouping_id\" BIGINT COMMENT 'Grouping id' ) COMMENT 'Example analytics table.' "
            "WITH ( partitioned_by = ARRAY[ 'ds', 'aggregation' ], format = 'ORC' )",
        )

    def test_no_with_when_no_props_and_no_partitions(self):
        t = PrestoBareMin(schema="s")
        self.assertEqual(
            norm_ws(PrestoDialect(t).get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"s\".\"presto_bare_min\" ( \"id\" INTEGER ) "
            "COMMENT 'No table properties and no partitions: CREATE without WITH.'",
        )

    def test_bucketed_with_clause(self):
        t = PrestoBucketed(schema="b", ds="'1'")
        self.assertEqual(
            norm_ws(PrestoDialect(t).get_create_table()),
            "CREATE TABLE IF NOT EXISTS \"b\".\"presto_bucketed\" ( \"metric\" VARCHAR, "
            "\"value\" DOUBLE, \"ds\" VARCHAR ) COMMENT 'Presto / Trino table with bucket + format "
            "for WITH clause permutations.' WITH ( partitioned_by = ARRAY[ 'ds' ], format = 'PARQUET', "
            "bucketed_by = ARRAY['metric'], bucket_count = 4 )",
        )


class TestPrestoGetDropTruncate(unittest.TestCase):
    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_drop_table_default_suffix(self):
        self.assertEqual(
            one_line(self.d.get_drop_table()),
            'DROP TABLE IF EXISTS "default"."presto_like_example"',
        )

    def test_drop_table_suffix(self):
        self.assertEqual(
            one_line(self.d.get_drop_table(suffix="_backup")),
            'DROP TABLE IF EXISTS "default"."presto_like_example_backup"',
        )

    def test_truncate_suffix(self):
        self.assertEqual(
            one_line(self.d.get_truncate_table(suffix="_tmp")),
            'TRUNCATE TABLE "default"."presto_like_example_tmp"',
        )


class TestPrestoGetDeleteFrom(unittest.TestCase):
    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_no_where_without_condition(self):
        self.assertEqual(
            norm_ws(self.d.get_delete_from()),
            'DELETE FROM "default"."presto_like_example"',
        )

    def test_where_without_params(self):
        self.assertEqual(
            one_line(self.d.get_delete_from(condition='"metric" = \'x\'', params=None)),
            'DELETE FROM "default"."presto_like_example" WHERE "metric" = \'x\'',
        )

    def test_where_with_params(self):
        self.assertEqual(
            one_line(
                self.d.get_delete_from(condition='"metric" = {m}', params={"m": "'x'"})
            ),
            'DELETE FROM "default"."presto_like_example" WHERE "metric" = \'x\'',
        )

    def test_suffix(self):
        self.assertEqual(
            one_line(
                self.d.get_delete_from(
                    suffix="_del", condition='"ds" = {ds}', params={"ds": "'1'"}
                )
            ),
            'DELETE FROM "default"."presto_like_example_del" WHERE "ds" = \'1\'',
        )


class TestPrestoGetSelect(unittest.TestCase):
    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_minimal(self):
        self.assertEqual(
            norm_ws(self.d.get_select()),
            "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example\"",
        )

    def test_suffix_condition_limit(self):
        self.assertEqual(
            norm_ws(
                self.d.get_select(
                    suffix="_v",
                    condition='"metric" <> \'\'',
                    limit=5,
                )
            ),
            "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example_v\" WHERE \"metric\" <> \'\' LIMIT 5",
        )

    def test_transforms_use_arbitrary(self):
        self.assertEqual(
            norm_ws(
                self.d.get_select(
                    transforms={"metric": "ARBITRARY({c})"},
                    filter_fn=lambda c: c.name == "metric",
                )
            ),
            'SELECT ARBITRARY("metric") AS "metric" FROM "default"."presto_like_example"',
        )

    def test_filter_fn(self):
        self.assertEqual(
            norm_ws(self.d.get_select(filter_fn=lambda c: c.name == "value")),
            'SELECT "value" FROM "default"."presto_like_example"',
        )


class TestPrestoInheritedPartitionHelpers(unittest.TestCase):
    """Methods implemented on ``Dialect`` but exercised via Presto."""

    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_get_select_current_partition_permutations(self):
        cases = [
            (
                {},
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27' "
                "AND \"aggregation\" = 'daily'",
            ),
            (
                {"ignored_partitions": ["aggregation"]},
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27'",
            ),
            (
                {"condition": '"metric" = {m}', "params": {"m": "'a'"}},
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27' "
                "AND \"aggregation\" = 'daily' AND \"metric\" = 'a'",
            ),
            (
                {"limit": 10},
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27' "
                "AND \"aggregation\" = 'daily' LIMIT 10",
            ),
            (
                {"suffix": "_s"},
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example_s\" WHERE \"ds\" = '2019-07-27' "
                "AND \"aggregation\" = 'daily'",
            ),
            (
                {
                    "transforms": {"value": "SUM({c})"},
                    "ignored_partitions": ["aggregation"],
                },
                "SELECT \"ds\", \"aggregation\", \"metric\", SUM(\"value\") AS \"value\", "
                "\"grouping_id\" FROM \"default\".\"presto_like_example\" "
                "WHERE \"ds\" = '2019-07-27'",
            ),
        ]
        for kw, expected in cases:
            with self.subTest(kw=kw):
                self.assertEqual(norm_ws(self.d.get_select_current_partition(**kw)), expected)

    def test_get_sample_column_value_uses_arbitrary(self):
        self.assertEqual(
            norm_ws(self.d.get_sample_column_value(ignored_partitions=["aggregation"])),
            "SELECT ARBITRARY(\"ds\") AS \"ds\", ARBITRARY(\"aggregation\") AS \"aggregation\", "
            "ARBITRARY(\"metric\") AS \"metric\", ARBITRARY(\"value\") AS \"value\", "
            "ARBITRARY(\"grouping_id\") AS \"grouping_id\" FROM \"default\".\"presto_like_example\" "
            "WHERE \"ds\" = '2019-07-27'",
        )

    def test_get_delete_current_partition_suffix_and_params(self):
        self.assertEqual(
            one_line(
                self.d.get_delete_current_partition(
                    suffix="_x",
                    ignored_partitions=["aggregation"],
                    condition='"metric" = {m}',
                    params={"m": "'z'"},
                )
            ),
            'DELETE FROM "default"."presto_like_example_x" WHERE "ds" = \'2019-07-27\' '
            'AND "metric" = \'z\'',
        )


class TestPrestoInsertViews(unittest.TestCase):
    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_insert_into_via_select_embed_true_false(self):
        expected = {
            True: (
                "INSERT INTO \"default\".\"presto_like_example_i\" ( \"ds\", \"aggregation\", "
                "\"metric\", \"value\", \"grouping_id\" ) SELECT '2019-07-27' AS \"ds\", "
                "'daily' AS \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM (SELECT 1) AS vw"
            ),
            False: (
                "INSERT INTO \"default\".\"presto_like_example_i\" ( \"ds\", \"aggregation\", "
                "\"metric\", \"value\", \"grouping_id\" ) SELECT '2019-07-27' AS \"ds\", "
                "'daily' AS \"aggregation\", \"metric\", \"value\", \"grouping_id\" FROM SELECT 1"
            ),
        }
        for embed, exp in expected.items():
            with self.subTest(embed=embed):
                self.assertEqual(
                    norm_ws(
                        self.d.get_insert_into_via_select(
                            "SELECT 1", embed_select=embed, suffix="_i"
                        )
                    ),
                    exp,
                )

    def test_insert_into_from_table_delegates(self):
        self.assertEqual(
            norm_ws(self.d.get_insert_into_from_table("other.t")),
            "INSERT INTO \"default\".\"presto_like_example\" ( \"ds\", \"aggregation\", "
            "\"metric\", \"value\", \"grouping_id\" ) SELECT '2019-07-27' AS \"ds\", "
            "'daily' AS \"aggregation\", \"metric\", \"value\", \"grouping_id\" FROM other.t",
        )

    def test_insert_filter_fn(self):
        self.assertEqual(
            norm_ws(
                self.d.get_insert_into_via_select(
                    "SELECT 1", filter_fn=lambda c: c.name == "metric"
                )
            ),
            "INSERT INTO \"default\".\"presto_like_example\" ( \"metric\" ) SELECT \"metric\" "
            "FROM (SELECT 1) AS vw",
        )

    def test_drop_current_partition_view_suffix(self):
        self.assertEqual(
            one_line(self.d.get_drop_current_partition_view(suffix="_snap")),
            'DROP VIEW IF EXISTS "default"."presto_like_example_snap"',
        )

    def test_create_current_partition_view_security_invoker(self):
        expected = {
            False: (
                "CREATE OR REPLACE VIEW \"default\".\"presto_like_example_v\" AS SELECT "
                "\"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27'"
            ),
            True: (
                "CREATE OR REPLACE VIEW \"default\".\"presto_like_example_v\" SECURITY INVOKER AS "
                "SELECT \"ds\", \"aggregation\", \"metric\", \"value\", \"grouping_id\" "
                "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27'"
            ),
        }
        for sec, exp in expected.items():
            with self.subTest(security_invoker=sec):
                self.assertEqual(
                    norm_ws(
                        self.d.get_create_current_partition_view(
                            suffix="_v",
                            security_invoker=sec,
                            ignored_partitions=["aggregation"],
                        )
                    ),
                    exp,
                )

    def test_create_current_partition_view_transforms(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_current_partition_view(
                    transforms={"metric": "COUNT({c})"},
                    ignored_partitions=["aggregation"],
                )
            ),
            "CREATE OR REPLACE VIEW \"default\".\"presto_like_example_latest\" AS SELECT "
            "\"ds\", \"aggregation\", COUNT(\"metric\") AS \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27'",
        )


class TestPrestoGetUpsertSelect(unittest.TestCase):
    def setUp(self):
        self.d = PrestoDialect(
            PrestoLikeExample(
                schema="default",
                ds="'2019-07-27'",
                aggregation="'daily'",
            )
        )

    def test_without_primary_keys_no_exists_clause(self):
        self.assertEqual(
            norm_ws(self.d.get_upsert_select("SELECT 1 AS x")),
            "WITH incremental_update AS ( SELECT 1 AS x ) SELECT \"metric\", \"value\", "
            "\"grouping_id\" FROM incremental_update UNION ALL SELECT \"metric\", \"value\", "
            "\"grouping_id\" FROM ( SELECT \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27' "
            "AND \"aggregation\" = 'daily' ) AS d",
        )

    def test_with_primary_keys_adds_not_exists(self):
        self.assertEqual(
            norm_ws(
                self.d.get_upsert_select(
                    "SELECT 1",
                    primary_keys=["metric"],
                    ignored_partitions=["aggregation"],
                )
            ),
            "WITH incremental_update AS ( SELECT 1 ) SELECT \"metric\", \"value\", \"grouping_id\" "
            "FROM incremental_update UNION ALL SELECT \"metric\", \"value\", \"grouping_id\" FROM "
            "( SELECT \"metric\", \"value\", \"grouping_id\" FROM \"default\".\"presto_like_example\" "
            "WHERE \"ds\" = '2019-07-27' ) AS d WHERE NOT EXISTS ( SELECT 1 FROM incremental_update "
            "AS u WHERE u.\"metric\" = d.\"metric\" )",
        )

    def test_upsert_with_transforms(self):
        self.assertEqual(
            norm_ws(
                self.d.get_upsert_select(
                    "SELECT 1",
                    primary_keys=["metric"],
                    transforms={"metric": "MAX({c})"},
                    ignored_partitions=["aggregation"],
                )
            ),
            "WITH incremental_update AS ( SELECT 1 ) SELECT \"metric\", \"value\", \"grouping_id\" "
            "FROM incremental_update UNION ALL SELECT \"metric\", \"value\", \"grouping_id\" FROM "
            "( SELECT MAX(\"metric\") AS \"metric\", \"value\", \"grouping_id\" "
            "FROM \"default\".\"presto_like_example\" WHERE \"ds\" = '2019-07-27' ) AS d "
            "WHERE NOT EXISTS ( SELECT 1 FROM incremental_update AS u WHERE u.\"metric\" = d.\"metric\" )",
        )


class TestPrestoColumnsIterator(unittest.TestCase):
    def test_columns_order_non_partition_before_partition(self):
        d = PrestoDialect(
            PrestoLikeExample(
                schema="s",
                ds="'1'",
                aggregation="'2'",
            )
        )
        names = [c.name for c in d.columns()]
        self.assertEqual(names[-2:], ["ds", "aggregation"])
        self.assertNotIn("ds", names[:-2])


if __name__ == "__main__":
    unittest.main()
