"""Exhaustive Hive dialect SQL generation tests."""

import unittest

from dbsa.hive import Table as HiveDialect

from dbsa.tests.sql_helpers import norm_ws, one_line
from dbsa.tests.tables import HiveLikeExample, HiveTwoPartitions


class TestHiveGetCreateTable(unittest.TestCase):
    def setUp(self):
        self.t = HiveLikeExample(schema="dw", ds="'2021-06-01'")
        self.d = HiveDialect(self.t)

    def test_managed_table(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table()),
            "CREATE TABLE IF NOT EXISTS `dw`.`hive_like_example` ( `metric` STRING COMMENT 'Metric name' ) "
            "COMMENT 'Hive example.' PARTITIONED BY ( `ds` STRING COMMENT 'Date key' ) STORED AS ORC",
        )

    def test_suffix(self):
        self.assertEqual(
            one_line(self.d.get_create_table(suffix="_arch")),
            "CREATE TABLE IF NOT EXISTS `dw`.`hive_like_example_arch` ( `metric` STRING COMMENT 'Metric name' ) "
            "COMMENT 'Hive example.' PARTITIONED BY ( `ds` STRING COMMENT 'Date key' ) STORED AS ORC",
        )

    def test_external_table_with_location_tblformat_tblproperties(self):
        self.assertEqual(
            norm_ws(
                self.d.get_create_table(
                    external_table=True,
                    hdfs_path="hdfs://nn/w/x",
                    tblformat="ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'",
                    tblproperties=["'prop'='1'", "'k'='v'"],
                    suffix="",
                )
            ),
            "CREATE EXTERNAL TABLE IF NOT EXISTS `dw`.`hive_like_example` ( `metric` STRING COMMENT 'Metric name' ) "
            "COMMENT 'Hive example.' PARTITIONED BY ( `ds` STRING COMMENT 'Date key' ) "
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS ORC LOCATION 'hdfs://nn/w/x' "
            "TBLPROPERTIES('prop'='1','k'='v')",
        )

    def test_filter_fn_on_create(self):
        self.assertEqual(
            norm_ws(self.d.get_create_table(filter_fn=lambda c: c.name == "metric")),
            "CREATE TABLE IF NOT EXISTS `dw`.`hive_like_example` ( `metric` STRING COMMENT 'Metric name' ) "
            "COMMENT 'Hive example.' PARTITIONED BY ( `ds` STRING COMMENT 'Date key' ) STORED AS ORC",
        )


class TestHiveGetDropTruncateMsck(unittest.TestCase):
    def setUp(self):
        self.d = HiveDialect(HiveLikeExample(schema="dw", ds="'1'"))

    def test_drop_purge_suffix(self):
        self.assertEqual(
            one_line(self.d.get_drop_table(suffix="_old")),
            "DROP TABLE IF EXISTS `dw`.`hive_like_example_old` PURGE",
        )

    def test_truncate_suffix(self):
        self.assertEqual(
            one_line(self.d.get_truncate_table(suffix="_t")),
            "TRUNCATE TABLE `dw`.`hive_like_example_t`",
        )

    def test_msck(self):
        self.assertEqual(
            one_line(self.d.get_msck_table(suffix="_m")),
            "MSCK REPAIR TABLE `dw`.`hive_like_example_m`",
        )


class TestHivePartitionDdl(unittest.TestCase):
    def setUp(self):
        self.d = HiveDialect(
            HiveTwoPartitions(schema="d", ds="'1'", region="'eu'"),
        )

    def test_add_partition_with_and_without_hdfs(self):
        self.assertEqual(
            norm_ws(self.d.get_add_current_partition(hdfs_path="/data/p")),
            "ALTER TABLE `d`.`hive_two_partitions` ADD IF NOT EXISTS PARTITION( `ds` = '1', `region` = 'eu' ) "
            "LOCATION '/data/p'",
        )
        self.assertEqual(
            norm_ws(self.d.get_add_current_partition()),
            "ALTER TABLE `d`.`hive_two_partitions` ADD IF NOT EXISTS PARTITION( `ds` = '1', `region` = 'eu' )",
        )

    def test_add_partition_ignored_and_params(self):
        self.assertEqual(
            norm_ws(
                self.d.get_add_current_partition(
                    ignored_partitions=["region"],
                    params={"ds": "'9'"},
                )
            ),
            "ALTER TABLE `d`.`hive_two_partitions` ADD IF NOT EXISTS PARTITION( `ds` = '9' )",
        )

    def test_delete_current_partition_suffix_params_ignored(self):
        self.assertEqual(
            norm_ws(
                self.d.get_delete_current_partition(
                    suffix="_ignored_by_template",
                    ignored_partitions=["region"],
                    params={"ds": "'2'"},
                )
            ),
            "ALTER TABLE `d`.`hive_two_partitions` DROP IF EXISTS PARTITION( `ds` = '2' ) PURGE",
        )


class TestHiveSelectInsertViews(unittest.TestCase):
    def setUp(self):
        self.d = HiveDialect(HiveLikeExample(schema="dw", ds="'1'"))

    def test_select_condition_limit_transforms_suffix(self):
        self.assertEqual(
            norm_ws(
                self.d.get_select(
                    condition="`metric` = 'a'",
                    limit=3,
                    suffix="_lv",
                    transforms={"metric": "MAX({c})"},
                )
            ),
            "SELECT `ds`, MAX(`metric`) AS `metric` FROM `dw`.`hive_like_example_lv` WHERE `metric` = 'a' LIMIT 3",
        )

    def test_get_select_current_partition(self):
        self.assertEqual(
            norm_ws(self.d.get_select_current_partition(params={"ds": "'9'"})),
            "SELECT `ds`, `metric` FROM `dw`.`hive_like_example` WHERE `ds` = '9'",
        )

    def test_insert_into_via_select_embed_variants(self):
        expected = {
            True: (
                "INSERT INTO `dw`.`hive_like_example_ins` PARTITION ( `ds` = '1' ) ( `metric` ) SELECT `metric` "
                "FROM (SELECT `m` FROM s) vw"
            ),
            False: (
                "INSERT INTO `dw`.`hive_like_example_ins` PARTITION ( `ds` = '1' ) ( `metric` ) SELECT `metric` "
                "FROM SELECT `m` FROM s"
            ),
        }
        for embed, exp in expected.items():
            with self.subTest(embed=embed):
                self.assertEqual(
                    norm_ws(
                        self.d.get_insert_into_via_select(
                            "SELECT `m` FROM s",
                            embed_select=embed,
                            suffix="_ins",
                        )
                    ),
                    exp,
                )

    def test_insert_into_from_table(self):
        self.assertEqual(
            norm_ws(self.d.get_insert_into_from_table("src.t")),
            "INSERT INTO `dw`.`hive_like_example` PARTITION ( `ds` = '1' ) ( `metric` ) SELECT `metric` FROM src.t",
        )

    def test_insert_overwrite_suffix(self):
        self.assertEqual(
            norm_ws(self.d.get_insert_overwrite_via_select("SELECT 1", suffix="_ow")),
            "INSERT OVERWRITE TABLE `dw`.`hive_like_example_ow` PARTITION ( `ds` = '1' ) SELECT 1",
        )

    def test_drop_create_partition_view(self):
        self.assertEqual(
            one_line(self.d.get_drop_current_partition_view(suffix="_lv")),
            "DROP VIEW IF EXISTS `dw`.`hive_like_example_lv`",
        )
        self.assertEqual(
            norm_ws(self.d.get_create_current_partition_view(suffix="_cv")),
            "CREATE OR REPLACE VIEW `dw`.`hive_like_example_cv` AS SELECT `ds`, `metric` FROM `dw`.`hive_like_example` "
            "WHERE `ds` = '1'",
        )


if __name__ == "__main__":
    unittest.main()
