"""Shared table prototypes for SQL generation tests."""

import dbsa


class PrestoLikeExample(dbsa.Table):
    """Example analytics table."""

    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar(), comment="Date key")
    aggregation = dbsa.Partition(dbsa.Varchar(), comment="Aggregation name")
    metric = dbsa.Varchar(comment="Metric name")
    grouping_id: dbsa.Bigint = dbsa.Column(comment="Grouping id")
    value = dbsa.Double(comment="Metric value")


class PrestoBucketed(dbsa.Table):
    """Presto / Trino table with bucket + format for WITH clause permutations."""

    _format = dbsa.Format(format="PARQUET")
    _bucket = dbsa.Bucket(by=["metric"], count=4)
    ds = dbsa.Partition(dbsa.Varchar())
    metric = dbsa.Varchar()
    value = dbsa.Double()


class HiveLikeExample(dbsa.Table):
    """Hive example."""

    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar(), comment="Date key")
    metric = dbsa.Varchar(comment="Metric name")


class HiveTwoPartitions(dbsa.Table):
    _format = dbsa.Format(format="ORC")
    ds = dbsa.Partition(dbsa.Varchar())
    region = dbsa.Partition(dbsa.Varchar())
    metric = dbsa.Varchar()


class RedshiftLikeExample(dbsa.Table):
    _sk = dbsa.Sortkey(keys=["ds", "metric"])
    ds = dbsa.Varchar(length=32, encode="zstd", comment="Date key")
    metric = dbsa.Varchar(length=256, encode="zstd", comment="Metric name")
    n: dbsa.Bigint = dbsa.Column(encode="raw", comment="Count")


class RedshiftPartitioned(dbsa.Table):
    """Redshift external-style layout with partition columns."""

    _sk = dbsa.Sortkey(keys=["ds"])
    ds = dbsa.Partition(dbsa.Varchar(length=16, encode="zstd"))
    event = dbsa.Varchar(length=128, encode="zstd")


class RedshiftPartManual(dbsa.Table):
    """Partitioned table with a non-partition column for UPDATE tests."""

    _sk = dbsa.Sortkey(keys=["ds"])
    ds = dbsa.Partition(dbsa.Varchar(length=16, encode="zstd"))
    status = dbsa.Varchar(length=64, encode="raw")


class RedshiftDistExample(dbsa.Table):
    _sk = dbsa.Sortkey(keys=["id"])
    _dk = dbsa.DistributionKey(key="id")
    _ds = dbsa.DistributionStyle(style="KEY")
    id = dbsa.Bigint(encode="raw")
    name = dbsa.Varchar(length=32, encode="zstd")


class RedshiftJsonPath(dbsa.Table):
    _sk = dbsa.Sortkey(keys=["id"])
    id = dbsa.Bigint(encode="raw")
    payload = dbsa.Varchar(length=1024, encode="zstd", jsonpath="$['a']")


class PrestoNoPartition(dbsa.Table):
    """Managed table without partitions (Trino WITH has only table props)."""

    _format = dbsa.Format(format="ORC")
    id = dbsa.Bigint(comment="pk")
    name = dbsa.Varchar(length=10, comment="name")


class PrestoBareMin(dbsa.Table):
    """No table properties and no partitions: CREATE without WITH."""

    id = dbsa.Integer()


class MarkdownDocTable(dbsa.Table):
    """Table used for markdown / policy tests."""

    _retention = dbsa.PartitionRetentionPolicy(
        ds_ago=30,
        earliest_partition={"ds": "'{{ macros.ds_add(ds, -7) }}'"},
    )
    ds = dbsa.Partition(dbsa.Varchar(), comment="Date")
    email = dbsa.Varchar(length=64, comment="PII email", pii=dbsa.DataType())
