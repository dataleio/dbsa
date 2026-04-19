"""Redshift physical types and distribution / sort properties."""

import unittest

from dbsa.redshift import Table as RedshiftDialect

from dbsa.tests.sql_helpers import norm_ws


class AllRedshiftPhysicalTypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import dbsa

        class RsTypes(dbsa.Table):
            _sk = dbsa.Sortkey(keys=["c_int"])
            c_bool = dbsa.Boolean()
            c_tiny = dbsa.Tinyint(encode="raw")
            c_small = dbsa.Smallint(encode="raw")
            c_int = dbsa.Integer(encode="raw")
            c_big = dbsa.Bigint(encode="raw")
            c_real = dbsa.Real(encode="raw")
            c_dbl = dbsa.Double(encode="raw")
            c_dec = dbsa.Decimal(precision=8, scale=2, encode="zstd")
            c_vc = dbsa.Varchar(length=16, encode="zstd")
            c_char = dbsa.Char(length=4, encode="zstd")
            c_date = dbsa.Date(encode="zstd")
            c_ts = dbsa.Timestamp(encode="zstd")

        cls.RsTypes = RsTypes

    def test_fragments(self):
        t = self.RsTypes(schema="rs")
        sql = norm_ws(RedshiftDialect(t).get_create_table())
        self.assertEqual(
            sql,
            'CREATE TABLE IF NOT EXISTS "rs"."rs_types" ( "c_bool" BOOLEAN, "c_tiny" TINYINT ENCODE RAW, '
            '"c_small" SMALLINT ENCODE RAW, "c_int" INTEGER ENCODE RAW, "c_big" BIGINT ENCODE RAW, '
            '"c_real" REAL ENCODE RAW, "c_dbl" FLOAT ENCODE RAW, "c_dec" NUMERIC(8,2) ENCODE ZSTD, '
            '"c_vc" VARCHAR(16) ENCODE ZSTD, "c_char" CHAR(4) ENCODE ZSTD, "c_date" DATE ENCODE ZSTD, '
            '"c_ts" TIMESTAMP ENCODE ZSTD ) SORTKEY("c_int");',
        )


if __name__ == "__main__":
    unittest.main()
