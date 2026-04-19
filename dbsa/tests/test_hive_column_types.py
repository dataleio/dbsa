"""Hive-specific physical types render in CREATE TABLE."""

import unittest

from dbsa.hive import Table as HiveDialect

from dbsa.tests.sql_helpers import norm_ws


class AllHivePhysicalTypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import dbsa

        ix = dbsa.Integer()
        ix.name = "x"
        iy = dbsa.Varchar(length=8)
        iy.name = "y"

        class HiveWide(dbsa.Table):
            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar())
            c_bool = dbsa.Boolean()
            c_tiny = dbsa.Tinyint()
            c_small = dbsa.Smallint()
            c_int = dbsa.Integer()
            c_big = dbsa.Bigint()
            c_real = dbsa.Real()
            c_dbl = dbsa.Double()
            c_dec = dbsa.Decimal(precision=10, scale=2)
            c_vc = dbsa.Varchar()
            c_vc_len = dbsa.Varchar(length=12)
            c_char = dbsa.Char(length=2)
            c_bin = dbsa.Varbinary(length=4)
            c_json = dbsa.JSON()
            c_date = dbsa.Date()
            c_ts = dbsa.Timestamp()
            c_arr = dbsa.Array(data_type=dbsa.Integer())
            c_map = dbsa.Map(primitive_type=dbsa.Varchar(), data_type=dbsa.Boolean())
            c_row = dbsa.Row(columns=[ix, iy])
            c_ip = dbsa.IPAddress()

        cls.HiveWide = HiveWide

    def test_create_table_type_fragments(self):
        t = self.HiveWide(schema="h", ds="'1'")
        sql = norm_ws(HiveDialect(t).get_create_table())
        self.assertEqual(
            sql,
            "CREATE TABLE IF NOT EXISTS `h`.`hive_wide` ( `c_bool` BOOLEAN, `c_tiny` TINYINT, "
            "`c_small` SMALLINT, `c_int` INT, `c_big` BIGINT, `c_real` FLOAT, `c_dbl` DOUBLE, "
            "`c_dec` DECIMAL(10,2), `c_vc` STRING, `c_vc_len` STRING(12), `c_char` CHAR(2), "
            "`c_bin` BINARY(4), `c_json` STRING, `c_date` DATE, `c_ts` TIMESTAMP, `c_arr` ARRAY<INT>, "
            "`c_map` MAP<STRING, BOOLEAN>, `c_row` STRUCT<`x` : INT, `y` : STRING(8)>, `c_ip` STRING ) "
            "PARTITIONED BY ( `ds` STRING ) STORED AS ORC",
        )


if __name__ == "__main__":
    unittest.main()
