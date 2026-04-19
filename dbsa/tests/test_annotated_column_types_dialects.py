"""
Every column type declared with ``name: dbsa.SomeType = dbsa.Column(...)``,
including dialect-specific required kwargs (encode, length, precision/scale, etc.).
"""

import unittest

import dbsa
from dbsa.hive import Table as HiveDialect
from dbsa.presto import Table as PrestoDialect
from dbsa.redshift import Table as RedshiftDialect
from dbsa.trino import Table as TrinoDialect
from dbsa.trino_iceberg import Table as IcebergDialect

from dbsa.tests.sql_helpers import norm_ws

# Row member columns (not table-level ``name: Type = Column`` — nested in ``Row``).
_row_x = dbsa.Integer()
_row_x.name = "x"
_row_y = dbsa.Varchar(length=8)
_row_y.name = "y"


class AnnotatedPrestoAllTypes(dbsa.Table):
    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar())
    c_bool: dbsa.Boolean = dbsa.Column(comment="b")
    c_tiny: dbsa.Tinyint = dbsa.Column()
    c_small: dbsa.Smallint = dbsa.Column()
    c_int: dbsa.Integer = dbsa.Column()
    c_big: dbsa.Bigint = dbsa.Column()
    c_real: dbsa.Real = dbsa.Column()
    c_dbl: dbsa.Double = dbsa.Column()
    c_dec: dbsa.Decimal = dbsa.Column(precision=12, scale=4)
    c_vc: dbsa.Varchar = dbsa.Column()
    c_vc_len: dbsa.Varchar = dbsa.Column(length=64)
    c_char: dbsa.Char = dbsa.Column(length=3)
    c_bin: dbsa.Varbinary = dbsa.Column(length=8)
    c_json: dbsa.JSON = dbsa.Column()
    c_date: dbsa.Date = dbsa.Column()
    c_time: dbsa.Time = dbsa.Column()
    c_ts: dbsa.Timestamp = dbsa.Column()
    c_ts_prec: dbsa.Timestamp = dbsa.Column(length=6)
    c_ts_tz: dbsa.Timestamp = dbsa.Column(with_timezone=True)
    c_arr: dbsa.Array[dbsa.Integer] = dbsa.Column()
    c_map: dbsa.Map[dbsa.Varchar(length=4), dbsa.Boolean()] = dbsa.Column()
    c_row: dbsa.Row = dbsa.Column(columns=[_row_x, _row_y])
    c_ip: dbsa.IPAddress = dbsa.Column()


class AnnotatedHiveAllTypes(dbsa.Table):
    """Hive has no ``Time``; ``Timestamp`` template is plain ``TIMESTAMP``."""

    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar())
    c_bool: dbsa.Boolean = dbsa.Column()
    c_tiny: dbsa.Tinyint = dbsa.Column()
    c_small: dbsa.Smallint = dbsa.Column()
    c_int: dbsa.Integer = dbsa.Column()
    c_big: dbsa.Bigint = dbsa.Column()
    c_real: dbsa.Real = dbsa.Column()
    c_dbl: dbsa.Double = dbsa.Column()
    c_dec: dbsa.Decimal = dbsa.Column(precision=10, scale=2)
    c_vc: dbsa.Varchar = dbsa.Column()
    c_vc_len: dbsa.Varchar = dbsa.Column(length=12)
    c_char: dbsa.Char = dbsa.Column(length=2)
    c_bin: dbsa.Varbinary = dbsa.Column(length=4)
    c_json: dbsa.JSON = dbsa.Column()
    c_date: dbsa.Date = dbsa.Column()
    c_ts: dbsa.Timestamp = dbsa.Column()
    c_arr: dbsa.Array[dbsa.Integer] = dbsa.Column()
    c_map: dbsa.Map[dbsa.Varchar, dbsa.Boolean] = dbsa.Column()
    c_row: dbsa.Row = dbsa.Column(columns=[_row_x, _row_y])
    c_ip: dbsa.IPAddress = dbsa.Column()


class AnnotatedRedshiftAllTypes(dbsa.Table):
    """Redshift: ``encode`` / ``length`` / ``precision``+``scale`` per ``_req_properties``."""

    _sk = dbsa.Sortkey(keys=["c_int"])
    c_bool: dbsa.Boolean = dbsa.Column()
    c_tiny: dbsa.Tinyint = dbsa.Column(encode="raw")
    c_small: dbsa.Smallint = dbsa.Column(encode="raw")
    c_int: dbsa.Integer = dbsa.Column(encode="raw")
    c_big: dbsa.Bigint = dbsa.Column(encode="raw")
    c_real: dbsa.Real = dbsa.Column(encode="raw")
    c_dbl: dbsa.Double = dbsa.Column(encode="raw")
    c_dec: dbsa.Decimal = dbsa.Column(precision=8, scale=2, encode="zstd")
    c_vc: dbsa.Varchar = dbsa.Column(length=32, encode="zstd")
    c_char: dbsa.Char = dbsa.Column(length=4, encode="zstd")
    c_date: dbsa.Date = dbsa.Column(encode="zstd")
    c_ts: dbsa.Timestamp = dbsa.Column(encode="zstd")


class TestAnnotatedPrestoTrinoIcebergColumnTypes(unittest.TestCase):
    """Presto / Trino / Iceberg share the same physical type strings in CREATE TABLE."""

    _EXPECTED_PRESTO_TRINO = (
        "CREATE TABLE IF NOT EXISTS \"ann\".\"annotated_presto_all_types\" ( "
        "\"c_bool\" BOOLEAN COMMENT 'b', \"c_tiny\" TINYINT, \"c_small\" SMALLINT, "
        "\"c_int\" INTEGER, \"c_big\" BIGINT, \"c_real\" REAL, \"c_dbl\" DOUBLE, "
        "\"c_dec\" DECIMAL(12,4), \"c_vc\" VARCHAR, \"c_vc_len\" VARCHAR(64), "
        "\"c_char\" CHAR(3), \"c_bin\" VARBINARY(8), \"c_json\" JSON, \"c_date\" DATE, "
        "\"c_time\" TIME, \"c_ts\" TIMESTAMP, \"c_ts_prec\" TIMESTAMP(6), "
        "\"c_ts_tz\" TIMESTAMP WITH TIME ZONE, \"c_arr\" ARRAY(INTEGER), "
        "\"c_map\" MAP(VARCHAR(4), BOOLEAN), \"c_row\" ROW(\"x\" INTEGER, \"y\" VARCHAR(8)), "
        "\"c_ip\" IPADDRESS, \"ds\" VARCHAR ) WITH ( partitioned_by = ARRAY[ 'ds' ], format = 'ORC' )"
    )

    _EXPECTED_ICEBERG = (
        "CREATE TABLE IF NOT EXISTS \"ann\".\"annotated_presto_all_types\" ( "
        "\"c_bool\" BOOLEAN COMMENT 'b', \"c_tiny\" TINYINT, \"c_small\" SMALLINT, "
        "\"c_int\" INTEGER, \"c_big\" BIGINT, \"c_real\" REAL, \"c_dbl\" DOUBLE, "
        "\"c_dec\" DECIMAL(12,4), \"c_vc\" VARCHAR, \"c_vc_len\" VARCHAR(64), "
        "\"c_char\" CHAR(3), \"c_bin\" VARBINARY(8), \"c_json\" JSON, \"c_date\" DATE, "
        "\"c_time\" TIME, \"c_ts\" TIMESTAMP, \"c_ts_prec\" TIMESTAMP(6), "
        "\"c_ts_tz\" TIMESTAMP WITH TIME ZONE, \"c_arr\" ARRAY(INTEGER), "
        "\"c_map\" MAP(VARCHAR(4), BOOLEAN), \"c_row\" ROW(\"x\" INTEGER, \"y\" VARCHAR(8)), "
        "\"c_ip\" IPADDRESS, \"ds\" VARCHAR ) WITH ( partitioning = ARRAY[ 'ds' ], format = 'ORC' )"
    )

    def setUp(self):
        self.table = AnnotatedPrestoAllTypes(schema="ann", ds="'1'")

    def test_presto_create_table(self):
        self.assertEqual(
            norm_ws(PrestoDialect(self.table).get_create_table()),
            self._EXPECTED_PRESTO_TRINO,
        )

    def test_trino_create_table_same_column_sql(self):
        self.assertEqual(
            norm_ws(TrinoDialect(self.table).get_create_table()),
            self._EXPECTED_PRESTO_TRINO,
        )

    def test_trino_iceberg_create_table_same_column_sql(self):
        self.assertEqual(
            norm_ws(IcebergDialect(self.table).get_create_table()),
            self._EXPECTED_ICEBERG,
        )


class TestAnnotatedHiveColumnTypes(unittest.TestCase):
    def test_hive_create_table(self):
        t = AnnotatedHiveAllTypes(schema="h", ds="'1'")
        self.assertEqual(
            norm_ws(HiveDialect(t).get_create_table()),
            "CREATE TABLE IF NOT EXISTS `h`.`annotated_hive_all_types` ( `c_bool` BOOLEAN, `c_tiny` TINYINT, "
            "`c_small` SMALLINT, `c_int` INT, `c_big` BIGINT, `c_real` FLOAT, `c_dbl` DOUBLE, "
            "`c_dec` DECIMAL(10,2), `c_vc` STRING, `c_vc_len` STRING(12), `c_char` CHAR(2), "
            "`c_bin` BINARY(4), `c_json` STRING, `c_date` DATE, `c_ts` TIMESTAMP, `c_arr` ARRAY<INT>, "
            "`c_map` MAP<STRING, BOOLEAN>, `c_row` STRUCT<`x` : INT, `y` : STRING(8)>, `c_ip` STRING ) "
            "COMMENT 'Hive has no ``Time``; ``Timestamp`` template is plain ``TIMESTAMP``.' "
            "PARTITIONED BY ( `ds` STRING ) STORED AS ORC",
        )


class TestAnnotatedRedshiftColumnTypes(unittest.TestCase):
    def test_redshift_create_table(self):
        t = AnnotatedRedshiftAllTypes(schema="rs")
        self.assertEqual(
            norm_ws(RedshiftDialect(t).get_create_table()),
            'CREATE TABLE IF NOT EXISTS "rs"."annotated_redshift_all_types" ( "c_bool" BOOLEAN, '
            '"c_tiny" TINYINT ENCODE RAW, "c_small" SMALLINT ENCODE RAW, "c_int" INTEGER ENCODE RAW, '
            '"c_big" BIGINT ENCODE RAW, "c_real" REAL ENCODE RAW, "c_dbl" FLOAT ENCODE RAW, '
            '"c_dec" NUMERIC(8,2) ENCODE ZSTD, "c_vc" VARCHAR(32) ENCODE ZSTD, '
            '"c_char" CHAR(4) ENCODE ZSTD, "c_date" DATE ENCODE ZSTD, "c_ts" TIMESTAMP ENCODE ZSTD ) '
            'SORTKEY("c_int");',
        )


if __name__ == "__main__":
    unittest.main()
