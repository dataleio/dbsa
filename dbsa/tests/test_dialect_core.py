"""Tests for ``Table`` / ``Dialect`` core helpers not tied to one SQL dialect."""

import unittest

import dbsa
from dbsa.presto import Table as PrestoDialect

from dbsa.tests.sql_helpers import norm_ws
from dbsa.tests.tables import MarkdownDocTable, PrestoLikeExample


class TestTableNaming(unittest.TestCase):
    def setUp(self):
        self.t_no_cat = PrestoLikeExample(schema="sch", ds="'1'", aggregation="'2'")
        self.t_cat = PrestoLikeExample(
            schema="sch",
            ds="'1'",
            aggregation="'2'",
            catalog="cat",
        )

    def test_full_table_name_permutations(self):
        cases = [
            (dict(quoted=False, with_prefix=False), "sch.presto_like_example"),
            (dict(quoted=True, with_prefix=False), '"sch"."presto_like_example"'),
            (dict(quoted=True, with_prefix=True, suffix="_x"), '"sch"."presto_like_example_x"'),
        ]
        for kw, expected in cases:
            with self.subTest(kw=kw):
                self.assertEqual(self.t_no_cat.full_table_name(**kw), expected)

    def test_full_table_name_with_catalog(self):
        self.assertEqual(
            self.t_cat.full_table_name(quoted=True, with_prefix=False),
            '"cat"."sch"."presto_like_example"',
        )

    def test_partition_definition_and_staging(self):
        self.assertEqual(self.t_no_cat.partition_definition(), "ds=1/aggregation=2")
        self.assertEqual(self.t_no_cat.staging_table_name(), "stg_1_2_presto_like_example")


class TestDialectCloneAndMarkdown(unittest.TestCase):
    def test_clone_builds_new_instance_with_partition_overrides(self):
        d = PrestoDialect(
            PrestoLikeExample(schema="a", ds="'old'", aggregation="'x'"),
        )
        d2 = d.clone(ds="'new'")
        self.assertEqual(d2.table.ds.value, "'new'")
        self.assertIsNot(d.table, d2.table)

    def test_to_markdown_contains_table_and_columns(self):
        d = PrestoDialect(
            PrestoLikeExample(schema="doc", ds="'1'", aggregation="'2'"),
        )
        md = d.to_markdown(header="##")
        self.assertEqual(
            norm_ws(md),
            "## doc.presto_like_example Example analytics table. | Column name | Column Type | PII | Description | "
            "| ----------- | ---- | --- | ----------- | | **ds** | `VARCHAR` | | Date key | | **aggregation** | "
            "`VARCHAR` | | Aggregation name | | metric | `VARCHAR` | | Metric name | | value | `DOUBLE` | | "
            "Metric value | | grouping_id | `BIGINT` | | Grouping id |",
        )

    def test_to_markdown_custom_header(self):
        d = PrestoDialect(
            PrestoLikeExample(schema="d", ds="'1'", aggregation="'2'"),
        )
        self.assertEqual(
            norm_ws(d.to_markdown(header="#")),
            "# d.presto_like_example Example analytics table. | Column name | Column Type | PII | Description | "
            "| ----------- | ---- | --- | ----------- | | **ds** | `VARCHAR` | | Date key | | **aggregation** | "
            "`VARCHAR` | | Aggregation name | | metric | `VARCHAR` | | Metric name | | value | `DOUBLE` | | "
            "Metric value | | grouping_id | `BIGINT` | | Grouping id |",
        )


class TestPartitionRetentionPolicyResolve(unittest.TestCase):
    def test_resolve_policy_returns_delete_sql(self):
        d = PrestoDialect(
            MarkdownDocTable(schema="p", ds="'2020-01-01'"),
        )
        sql = d.resolve_policy(dbsa.PartitionRetentionPolicy)
        self.assertIsNotNone(sql)
        self.assertEqual(
            norm_ws(sql),
            'DELETE FROM "p"."markdown_doc_table" WHERE "ds" = \'{{ macros.ds_add(ds, -7) }}\'',
        )


class TestDialectAddTableColumn(unittest.TestCase):
    def test_add_table_column_registers_with_dialect(self):
        d = PrestoDialect(
            PrestoLikeExample(schema="s", ds="'1'", aggregation="'2'"),
        )
        extra = dbsa.Boolean(comment="extra")
        extra.name = "extra_col"
        d.add_table_column(extra)
        self.assertEqual(
            d.column_names(as_list=True),
            ["ds", "aggregation", "metric", "value", "grouping_id", "extra_col"],
        )


class TestCleanupFn(unittest.TestCase):
    def test_cleanup_fn_strips_quotes_and_ds_nodash_when_dashed_false(self):
        v = dbsa.cleanup_fn("'{{ ds }}'", quoted=False, dashed=False)
        self.assertEqual(v, "{{ ds_nodash }}")


if __name__ == "__main__":
    unittest.main()
