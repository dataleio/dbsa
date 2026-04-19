"""``ExternalTableProperties`` rendering for Trino / Iceberg."""

import unittest

from dbsa.trino import ExternalTableProperties as TrinoEtp
from dbsa.trino_iceberg import ExternalTableProperties as IcebergEtp


class TestTrinoExternalTableProperties(unittest.TestCase):
    def test_default_external_location_and_string_config(self):
        p = TrinoEtp("s3://bucket/p", configs={"k": "v"})
        props = p.get_properies()
        self.assertEqual(props[0], "external_location = 's3://bucket/p'")
        self.assertEqual(props[1], "k = 'v'")

    def test_numeric_config_unquoted(self):
        p = TrinoEtp("loc", configs={"n": 7})
        self.assertEqual(p.get_properies()[1], "n = 7")

    def test_custom_location_property_name(self):
        p = TrinoEtp("x", configs={}, location_property_name="my_loc")
        self.assertEqual(p.get_properies(), ["my_loc = 'x'"])


class TestIcebergExternalTableProperties(unittest.TestCase):
    def test_uses_location_not_external_location(self):
        p = IcebergEtp("s3://ice/", configs={"table_type": "ICEBERG"})
        self.assertEqual(
            p.get_properies(),
            ["location = 's3://ice/'", "table_type = 'ICEBERG'"],
        )


if __name__ == "__main__":
    unittest.main()
