import unittest

import dbsa
from dbsa.presto import Table as PrestoDialect


def _squash_ws(s):
    return " ".join(s.split())


class TestAnnotatedColumnSyntax(unittest.TestCase):
    def test_annotation_default_equals_column(self):
        class Metrics(dbsa.Table):
            """Docstring for table."""

            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar(), comment="partition ds")
            grouping_id: dbsa.Bigint = dbsa.Column(comment="abc")
            metric = dbsa.Varchar(comment="legacy style")

        t = Metrics(schema="default", ds="'2019-07-27'")
        self.assertEqual(t.grouping_id.comment, "abc")
        self.assertIsInstance(t.grouping_id, dbsa.Bigint)
        self.assertEqual(t.grouping_id.name, "grouping_id")

        d = PrestoDialect(t)
        sql = _squash_ws(d.get_create_table())
        self.assertEqual(
            sql,
            "CREATE TABLE IF NOT EXISTS \"default\".\"metrics\" ( \"metric\" VARCHAR COMMENT 'legacy style', "
            "\"grouping_id\" BIGINT COMMENT 'abc', \"ds\" VARCHAR COMMENT 'partition ds' ) "
            "COMMENT 'Docstring for table.' WITH ( partitioned_by = ARRAY[ 'ds' ], format = 'ORC' )",
        )

    def test_annotation_with_extra_kwargs(self):
        class T(dbsa.Table):
            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar())
            n: dbsa.Decimal = dbsa.Column(precision=10, scale=2, comment="dec")

        inst = T(schema="s", ds="'1'")
        self.assertEqual(inst.n.attrs.get("precision"), 10)
        self.assertEqual(inst.n.attrs.get("scale"), 2)

    def test_array_bracket_element_type_in_annotation(self):
        class T(dbsa.Table):
            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar())
            xs: dbsa.Array[dbsa.Integer] = dbsa.Column(comment="arr")

        t = T(schema="s", ds="'1'")
        self.assertIsInstance(t.xs, dbsa.Array)
        self.assertIsInstance(t.xs.attrs["data_type"], dbsa.Integer)
        self.assertEqual(t.xs.comment, "arr")

    def test_map_bracket_key_value_types_in_annotation(self):
        class T(dbsa.Table):
            _format = dbsa.Format(format="ORC")
            ds = dbsa.Partition(dbsa.Varchar())
            m: dbsa.Map[dbsa.Varchar(length=8), dbsa.Boolean()] = dbsa.Column(comment="kv")

        t = T(schema="s", ds="'1'")
        self.assertIsInstance(t.m, dbsa.Map)
        self.assertIsInstance(t.m.attrs["primitive_type"], dbsa.Varchar)
        self.assertEqual(t.m.attrs["primitive_type"].attrs.get("length"), 8)
        self.assertIsInstance(t.m.attrs["data_type"], dbsa.Boolean)
        self.assertEqual(t.m.comment, "kv")

    def test_bare_column_without_annotation_errors(self):
        with self.assertRaises(dbsa.BareColumnRequiresConcreteAnnotation):

            class Bad(dbsa.Table):  # noqa: D401
                _format = dbsa.Format(format="ORC")
                ds = dbsa.Partition(dbsa.Varchar())
                x = dbsa.Column(comment="no annotation")

    def test_partition_cannot_use_annotation_column_default(self):
        with self.assertRaises(dbsa.BareColumnRequiresConcreteAnnotation):

            class Bad(dbsa.Table):  # noqa: D401
                _format = dbsa.Format(format="ORC")
                ds: dbsa.Partition = dbsa.Column(comment="x")
                v = dbsa.Varchar()

    def test_postponed_evaluation_annotations(self):
        """``from __future__ import annotations`` stores string annotations; these must still work."""
        src = """
from __future__ import annotations
import dbsa

class FutureTable(dbsa.Table):
    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar())
    k: dbsa.Bigint = dbsa.Column(comment="postponed")
"""
        g = {"dbsa": dbsa}
        exec(compile(src, "<test_postponed>", "exec"), g, g)
        FutureTable = g["FutureTable"]
        inst = FutureTable(schema="s", ds="'1'")
        self.assertIsInstance(inst.k, dbsa.Bigint)
        self.assertEqual(inst.k.comment, "postponed")
