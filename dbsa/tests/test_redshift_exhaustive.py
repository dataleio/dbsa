"""Exhaustive Amazon Redshift dialect SQL generation tests."""

import json
import unittest

from dbsa.redshift import Table as RedshiftDialect

from dbsa.tests.sql_helpers import norm_ws, one_line
from dbsa.tests.tables import (
    RedshiftDistExample,
    RedshiftJsonPath,
    RedshiftLikeExample,
    RedshiftPartManual,
    RedshiftPartitioned,
)


class TestRedshiftJsonpathProperty(unittest.TestCase):
    def test_jsonpath_serializes_column_paths(self):
        d = RedshiftDialect(RedshiftJsonPath(schema="public"))
        data = json.loads(d.jsonpath)
        self.assertEqual(data["jsonpaths"], ["$['a']"])


class TestRedshiftGetCreateTable(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftLikeExample(schema="public"))

    def test_default(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example" ( "ds" VARCHAR(32) ENCODE ZSTD, '
            '"metric" VARCHAR(256) ENCODE ZSTD, "n" BIGINT ENCODE RAW ) SORTKEY("ds", "metric");',
        )

    def test_suffix_and_filter(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_table(suffix="_s", filter_fn=lambda c: c.name == "n")
            ),
            'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example_s" ( "n" BIGINT ENCODE RAW ) '
            'SORTKEY("ds", "metric");',
        )


class TestRedshiftGetCreateTableAs(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftLikeExample(schema="public"))

    def test_embed_select_true_false(self):
        expected = {
            True: (
                'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example_as" SORTKEY("ds", "metric") '
                'AS SELECT "ds", "metric", "n" FROM (SELECT 1) AS vw;'
            ),
            False: (
                'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example_as" SORTKEY("ds", "metric") '
                'AS SELECT "ds", "metric", "n" FROM SELECT 1;'
            ),
        }
        for embed, exp in expected.items():
            with self.subTest(embed=embed):
                self.assertEqual(
                    norm_ws(
                        self.d.get_create_table_as(
                            "SELECT 1",
                            embed_select=embed,
                            suffix="_as",
                        )
                    ),
                    exp,
                )

    def test_filter_fn_limits_columns_in_select_list(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_table_as(
                    "SELECT 1",
                    embed_select=True,
                    filter_fn=lambda c: c.name == "metric",
                )
            ),
            'CREATE TABLE IF NOT EXISTS "public"."redshift_like_example" SORTKEY("ds", "metric") '
            'AS SELECT "metric" FROM (SELECT 1) AS vw;',
        )


class TestRedshiftGetCreateExternalTable(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(
            RedshiftPartitioned(schema="ext", ds="'2020-01-01'"),
        )

    def test_partitioned_external(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_external_table(
                    hdfs_path="s3://x/y/",
                    fileformat="PARQUET",
                    tblformat="ROW FORMAT SERDE 'x'",
                    suffix="_e",
                )
            ),
            'CREATE EXTERNAL TABLE "ext"."redshift_partitioned_e" ( "event" VARCHAR(128) ) '
            'PARTITIONED BY ( "ds" VARCHAR(16) ) ROW FORMAT SERDE \'x\' STORED AS PARQUET '
            "LOCATION 's3://x/y/'",
        )

    def test_tblproperties_and_filter(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_external_table(
                    hdfs_path="s3://b/",
                    fileformat="TEXTFILE",
                    tblformat="",
                    tblproperties=["'a'='1'"],
                    filter_fn=lambda c: c.name == "event",
                )
            ),
            'CREATE EXTERNAL TABLE "ext"."redshift_partitioned" ( "event" VARCHAR(128) ) '
            'PARTITIONED BY ( "ds" VARCHAR(16) ) STORED AS TEXTFILE LOCATION \'s3://b/\' '
            "TABLE PROPERTIES ('a'='1')",
        )


class TestRedshiftGetCreateStagingTable(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(
            RedshiftPartitioned(schema="s", ds="'d'"),
        )

    def test_include_partitions_false_true(self):
        expected = {
            False: (
                'CREATE TABLE IF NOT EXISTS "s"."stg_d_redshift_partitioned_st" ( "event" VARCHAR(128) ENCODE ZSTD );'
            ),
            True: (
                'CREATE TABLE IF NOT EXISTS "s"."stg_d_redshift_partitioned_st" ( "ds" VARCHAR(16) ENCODE ZSTD, '
                '"event" VARCHAR(128) ENCODE ZSTD );'
            ),
        }
        for inc, exp in expected.items():
            with self.subTest(include_partitions=inc):
                self.assertEqual(
                    norm_ws(
                        self.d.get_create_staging_table(
                            include_partitions=inc,
                            suffix="_st",
                        )
                    ),
                    exp,
                )

    def test_cleanup_fn_changes_staging_name(self):
        def _fixed(v, quoted, dashed):
            return "fixed"

        self.assertEqual(
            norm_ws(self.d.get_create_staging_table()),
            'CREATE TABLE IF NOT EXISTS "s"."stg_d_redshift_partitioned" ( "event" VARCHAR(128) ENCODE ZSTD );',
        )
        self.assertEqual(
            norm_ws(self.d.get_create_staging_table(cleanup_fn=_fixed)),
            'CREATE TABLE IF NOT EXISTS "s"."stg_fixed_redshift_partitioned" ( "event" VARCHAR(128) ENCODE ZSTD );',
        )


class TestRedshiftPartitionAlter(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(
            RedshiftPartitioned(schema="p", ds="'1'"),
        )

    def test_add_external_partition(self):
        self.assertEqual(
            one_line(
                self.d.get_add_external_current_partition(
                    hdfs_path="s3://loc/",
                    params={"ds": "'9'"},
                )
            ),
            'ALTER TABLE "p"."redshift_partitioned" ADD IF NOT EXISTS PARTITION( "ds" = \'9\' ) '
            "LOCATION 's3://loc/'",
        )

    def test_delete_external_partition_suffix(self):
        self.assertEqual(
            one_line(
                self.d.get_delete_external_current_partition(
                    suffix="_z",
                    params={"ds": "'1'"},
                )
            ),
            'ALTER TABLE "p"."redshift_partitioned" DROP IF EXISTS PARTITION( "ds" = \'1\' )',
        )


class TestRedshiftDropTruncate(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftLikeExample(schema="public"))

    def test_drop_table_suffix(self):
        self.assertEqual(
            one_line(self.d.get_drop_table(suffix="_d")),
            'DROP TABLE IF EXISTS "public"."redshift_like_example_d";',
        )

    def test_drop_staging_suffix(self):
        t = RedshiftLikeExample(schema="s", ds="'1'", aggregation="'2'")
        self.assertEqual(
            one_line(RedshiftDialect(t).get_drop_staging_table(suffix="_st")),
            'DROP TABLE IF EXISTS "s"."stg_redshift_like_example_st";',
        )

    def test_truncate_suffix(self):
        self.assertEqual(
            one_line(self.d.get_truncate_table(suffix="_t")),
            'TRUNCATE TABLE "public"."redshift_like_example_t";',
        )


class TestRedshiftUpdate(unittest.TestCase):
    def test_update_manually_set_columns(self):
        t = RedshiftPartManual(schema="u", ds="'1'")
        t.status.set_column_value("'done'")
        d = RedshiftDialect(t)
        self.assertEqual(
            norm_ws(d.get_update_current_partition_for_manually_set_columns()),
            'UPDATE "u"."redshift_part_manual" SET "status" = \'done\' WHERE "ds" = \'1\'',
        )

    def test_update_returns_empty_when_no_manual_columns(self):
        t = RedshiftPartManual(schema="u", ds="'1'")
        d = RedshiftDialect(t)
        self.assertEqual(d.get_update_current_partition_for_manually_set_columns(), "")


class TestRedshiftCopy(unittest.TestCase):
    def test_copy_raw_template_sections(self):
        d = RedshiftDialect(RedshiftLikeExample(schema="s", ds="'1'", aggregation="'2'"))
        sql = d.get_copy_to_staging(suffix="_c", include_partitions=True)
        self.assertEqual(
            norm_ws(sql),
            'COPY "s"."stg_redshift_like_example_c" ( "ds", "metric", "n" ) '
            "FROM '{{ '{{ path_prefix }}://{{ path }}' }}' {{ '{% if access_key and secret_key %}' }} "
            "WITH CREDENTIALS '{{ 'aws_access_key_id={{ access_key }};aws_secret_access_key={{ secret_key }}' }}' "
            "{{ '{% else %}' }} IAM_ROLE '{{ '{{ iam_role }}' }}' {{ '{% endif %}' }} {{ '{{ copy_options }}' }} ;",
        )


class TestRedshiftSelect(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftDistExample(schema="public"))

    def test_use_star(self):
        self.assertEqual(
            norm_ws(self.d.get_select(use_star=True)),
            'SELECT * FROM "public"."redshift_dist_example"',
        )

    def test_order_by_sortkey(self):
        self.assertEqual(
            norm_ws(self.d.get_select(order_by_sortkey=True)),
            'SELECT "id", "name" FROM "public"."redshift_dist_example" ORDER BY "id"',
        )

    def test_transforms_limit_condition_suffix(self):
        self.assertEqual(
            norm_ws(
                self.d.get_select(
                    transforms={"name": "MIN({c})"},
                    limit=1,
                    condition='"id" > 0',
                    suffix="_v",
                )
            ),
            'SELECT "id", MIN("name") AS "name" FROM "public"."redshift_dist_example_v" WHERE "id" > 0 LIMIT 1',
        )


class TestRedshiftUnload(unittest.TestCase):
    def test_unload_table_delegates_to_select(self):
        d = RedshiftDialect(RedshiftLikeExample(schema="public"))
        tpl = d.get_unload_table()
        self.assertEqual(
            norm_ws(tpl.render()),
            "UNLOAD (' SELECT \"ds\", \"metric\", \"n\" FROM \"public\".\"redshift_like_example\" ') TO "
            "'s3:///' IAM_ROLE '' ;",
        )

    def test_unload_via_select_classmethod_renders(self):
        tpl = RedshiftDialect.get_unload_via_select("SELECT 1")
        self.assertEqual(
            norm_ws(tpl.render()),
            "UNLOAD (' SELECT 1 ') TO 's3:///' IAM_ROLE '' ;",
        )


class TestRedshiftDelete(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftLikeExample(schema="public"))

    def test_delete_from_variants(self):
        self.assertEqual(
            one_line(self.d.get_delete_from()),
            'DELETE FROM "public"."redshift_like_example";',
        )
        self.assertEqual(
            norm_ws(self.d.get_delete_from(using="other.t", condition="a = b")),
            'DELETE FROM "public"."redshift_like_example" USING other.t AS u WHERE a = b;',
        )
        self.assertEqual(
            one_line(self.d.get_delete_from(condition="x = {v}", params={"v": "1"})),
            'DELETE FROM "public"."redshift_like_example" WHERE x = 1;',
        )


class TestRedshiftDeleteUpsert(unittest.TestCase):
    def test_default_using_staging_name(self):
        t = RedshiftLikeExample(schema="s", ds="'1'", aggregation="'2'")
        d = RedshiftDialect(t)
        self.assertEqual(
            norm_ws(d.get_delete_upsert(pk_columns=["metric"])),
            'DELETE FROM "s"."redshift_like_example" USING "s"."stg_redshift_like_example" AS u '
            'WHERE u."metric" = "s"."redshift_like_example"."metric";',
        )

    def test_explicit_using(self):
        d = RedshiftDialect(RedshiftLikeExample(schema="s", ds="'1'", aggregation="'2'"))
        self.assertEqual(
            norm_ws(d.get_delete_upsert(pk_columns=["metric"], using='"stg"."other"')),
            'DELETE FROM "s"."redshift_like_example" USING "stg"."other" AS u WHERE u."metric" = '
            '"s"."redshift_like_example"."metric";',
        )


class TestRedshiftInsertViews(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(
            RedshiftLikeExample(schema="s", ds="'1'", aggregation="'2'"),
        )

    def test_insert_into_via_select_embed(self):
        expected = {
            True: (
                'INSERT INTO "s"."redshift_like_example" ( "ds", "metric", "n" ) SELECT "ds", "metric", "n" '
                "FROM (SELECT 1) AS vw;"
            ),
            False: (
                'INSERT INTO "s"."redshift_like_example" ( "ds", "metric", "n" ) SELECT "ds", "metric", "n" '
                "FROM SELECT 1;"
            ),
        }
        for embed, exp in expected.items():
            with self.subTest(embed=embed):
                self.assertEqual(
                    norm_ws(self.d.get_insert_into_via_select("SELECT 1", embed_select=embed)),
                    exp,
                )

    def test_insert_into_from_table(self):
        self.assertEqual(
            norm_ws(self.d.get_insert_into_from_table("z.t")),
            'INSERT INTO "s"."redshift_like_example" ( "ds", "metric", "n" ) SELECT "ds", "metric", "n" FROM z.t;',
        )

    def test_partition_views(self):
        self.assertEqual(
            one_line(self.d.get_drop_current_partition_view(suffix="_lv")),
            'DROP VIEW IF EXISTS "s"."redshift_like_example_lv";',
        )
        self.assertEqual(
            norm_ws(self.d.get_create_current_partition_view(suffix="_cv")),
            'CREATE OR REPLACE VIEW "s"."redshift_like_example_cv" AS SELECT "ds", "metric", "n" '
            'FROM "s"."redshift_like_example" ;',
        )


class TestRedshiftMaterializedViews(unittest.TestCase):
    def setUp(self):
        self.d = RedshiftDialect(RedshiftDistExample(schema="public"))

    def test_create_materialized_view_embed(self):
        expected = {
            True: (
                'CREATE MATERIALIZED VIEW "public"."redshift_dist_example_mv" SORTKEY("id") DISTKEY("id") '
                'DISTSTYLE KEY AS SELECT "id", "name" FROM (SELECT 1) AS vw;'
            ),
            False: (
                'CREATE MATERIALIZED VIEW "public"."redshift_dist_example_mv" SORTKEY("id") DISTKEY("id") '
                'DISTSTYLE KEY AS SELECT "id", "name" FROM SELECT 1;'
            ),
        }
        for embed, exp in expected.items():
            with self.subTest(embed=embed):
                self.assertEqual(
                    norm_ws(
                        self.d.get_create_materialized_view_via_select(
                            "SELECT 1",
                            embed_select=embed,
                            suffix="_mv",
                        )
                    ),
                    exp,
                )

    def test_drop_and_refresh_materialized_view(self):
        self.assertEqual(
            one_line(self.d.get_drop_materialized_view(suffix="_m")),
            'DROP MATERIALIZED VIEW "public"."redshift_dist_example_m";',
        )
        self.assertEqual(
            one_line(self.d.get_refresh_materialized_view(suffix="_m")),
            'REFRESH MATERIALIZED VIEW "public"."redshift_dist_example_m";',
        )


class TestRedshiftInheritedHelpers(unittest.TestCase):
    def test_get_select_current_partition_and_sample(self):
        d = RedshiftDialect(RedshiftPartitioned(schema="s", ds="'1'"))
        self.assertEqual(
            norm_ws(d.get_select_current_partition()),
            'SELECT "ds", "event" FROM "s"."redshift_partitioned" WHERE "ds" = \'1\'',
        )
        self.assertEqual(
            norm_ws(d.get_sample_column_value()),
            'SELECT MAX("ds") AS "ds", MAX("event") AS "event" FROM "s"."redshift_partitioned" '
            'WHERE "ds" = \'1\'',
        )


if __name__ == "__main__":
    unittest.main()
