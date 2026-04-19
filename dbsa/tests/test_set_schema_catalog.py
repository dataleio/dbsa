"""Table ``set_schema`` / ``set_catalog`` affect generated identifiers."""

import unittest

from dbsa.presto import Table as PrestoDialect

from dbsa.tests.sql_helpers import norm_ws
from dbsa.tests.tables import PrestoLikeExample


class TestSetSchemaAndCatalog(unittest.TestCase):
    def test_set_schema_updates_sql(self):
        t = PrestoLikeExample(schema="old", ds="'1'", aggregation="'2'")
        d = PrestoDialect(t)
        self.assertEqual(
            norm_ws(d.get_truncate_table()),
            'TRUNCATE TABLE "old"."presto_like_example"',
        )
        t.set_schema("new")
        self.assertEqual(
            norm_ws(d.get_truncate_table()),
            'TRUNCATE TABLE "new"."presto_like_example"',
        )

    def test_set_catalog_triple_qualified_name(self):
        t = PrestoLikeExample(
            schema="s",
            ds="'1'",
            aggregation="'2'",
            catalog="c1",
        )
        d = PrestoDialect(t)
        self.assertEqual(
            norm_ws(d.get_truncate_table()),
            'TRUNCATE TABLE "c1"."s"."presto_like_example"',
        )
        t.set_catalog("c2")
        self.assertEqual(
            norm_ws(d.get_truncate_table()),
            'TRUNCATE TABLE "c2"."s"."presto_like_example"',
        )


if __name__ == "__main__":
    unittest.main()
