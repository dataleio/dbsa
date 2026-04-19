"""Ensure every Presto ``_column_types`` mapping renders in CREATE TABLE."""

import unittest

from dbsa.presto import Table as PrestoDialect

from dbsa.tests.sql_helpers import norm_ws


class AllPrestoPhysicalTypes(unittest.TestCase):
    """One column per supported Presto type (plus common attribute variants)."""

    @classmethod
    def setUpClass(cls):
        import dbsa

        ix = dbsa.Integer()
        ix.name = "x"
        iy = dbsa.Varchar(length=8)
        iy.name = "y"

        class WideTypes(dbsa.Table):
            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar())
            c_bool = dbsa.Boolean(comment="b")
            c_tiny = dbsa.Tinyint()
            c_small = dbsa.Smallint()
            c_int = dbsa.Integer()
            c_big = dbsa.Bigint()
            c_real = dbsa.Real()
            c_dbl = dbsa.Double()
            c_dec = dbsa.Decimal(precision=12, scale=4)
            c_vc = dbsa.Varchar()
            c_vc_len = dbsa.Varchar(length=64)
            c_char = dbsa.Char(length=3)
            c_bin = dbsa.Varbinary(length=8)
            c_json = dbsa.JSON()
            c_date = dbsa.Date()
            c_time = dbsa.Time()
            c_ts = dbsa.Timestamp()
            c_ts_prec = dbsa.Timestamp(length=6)
            c_ts_tz = dbsa.Timestamp(with_timezone=True)
            c_arr = dbsa.Array(data_type=dbsa.Integer())
            c_map = dbsa.Map(primitive_type=dbsa.Varchar(length=4), data_type=dbsa.Boolean())
            c_row = dbsa.Row(columns=[ix, iy])
            c_ip = dbsa.IPAddress()

        cls.WideTypes = WideTypes

    def test_create_table_contains_each_type_keyword(self):
        t = self.WideTypes(schema="t", ds="'1'")
        d = PrestoDialect(t)
        sql = norm_ws(d.get_create_table())
        self.assertEqual(
            sql,
            "CREATE TABLE IF NOT EXISTS \"t\".\"wide_types\" ( \"c_bool\" BOOLEAN COMMENT 'b', "
            "\"c_tiny\" TINYINT, \"c_small\" SMALLINT, \"c_int\" INTEGER, \"c_big\" BIGINT, "
            "\"c_real\" REAL, \"c_dbl\" DOUBLE, \"c_dec\" DECIMAL(12,4), \"c_vc\" VARCHAR, "
            "\"c_vc_len\" VARCHAR(64), \"c_char\" CHAR(3), \"c_bin\" VARBINARY(8), \"c_json\" JSON, "
            "\"c_date\" DATE, \"c_time\" TIME, \"c_ts\" TIMESTAMP, \"c_ts_prec\" TIMESTAMP(6), "
            "\"c_ts_tz\" TIMESTAMP WITH TIME ZONE, \"c_arr\" ARRAY(INTEGER), "
            "\"c_map\" MAP(VARCHAR(4), BOOLEAN), \"c_row\" ROW(\"x\" INTEGER, \"y\" VARCHAR(8)), "
            "\"c_ip\" IPADDRESS, \"ds\" VARCHAR ) WITH ( partitioned_by = ARRAY[ 'ds' ], format = 'ORC' )",
        )


if __name__ == "__main__":
    unittest.main()
