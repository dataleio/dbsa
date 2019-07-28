# dbsa

Database schemas for Airflow. One of the biggest issue with [Apache Airflow](https://airflow.apache.org/index.html) that it does not provide any good way to describe the database schemas within the system. It leads incorrect table definitions, hard to extend schemas and keep backwards compatibility of already existing pipelines.

This package was written in mind to **use as the abstraction layer of table schemas**, and it provides support for [Presto](http://prestodb.github.io/), [Apache Hive](https://hive.apache.org/), and [Amazon Redshift](https://aws.amazon.com/redshift/).

## Installation

Installation is as simple as installing any other [Python packages](https://www.python.org/). 

```bash
$ pip install dbsa
```

## Supported column types

The following column types are supported:

| Date type | Presto support | Hive support | Redshift support |
| --------- | -------------- | ------------ | ---------------- |
| `dbsa.Boolean` | ✓ | ✓ | ✓ |
| `dbsa.Tinyint` | ✓ | ✓ | ✓ |
| `dbsa.Smallint` | ✓ | ✓ | ✓ |
| `dbsa.Integer` | ✓ | ✓ | ✓ |
| `dbsa.Bigint` | ✓ | ✓ | ✓ |
| `dbsa.Real` | ✓ | ✓ | ✓ |
| `dbsa.Double` | ✓ | ✓ | ✓ |
| `dbsa.Decimal` | ✓ | ✓ | ✓ |
| `dbsa.Varchar` | ✓ | ✓ | ✓ |
| `dbsa.Char` | ✓ | ✓ | ✓ |
| `dbsa.Varbinary` | ✓ | ✓ | |
| `dbsa.JSON` | ✓ | ✓ AS `Varchar` | |
| `dbsa.Date` | ✓ | ✓ | ✓ |
| `dbsa.Time` | ✓ | | |
| `dbsa.Timestamp` | ✓ | ✓ | ✓ |
| `dbsa.Array` | ✓ | ✓ | |
| `dbsa.Map` | ✓ | ✓ | |
| `dbsa.Row` | ✓ | ✓ | |
| `dbsa.IPAddress` | ✓ | ✓ AS `Varchar` | |


## Supported Table Properties

The following table properties are supported:

| Date type | Presto support | Hive support | Redshift support |
| --------- | -------------- | ------------ | ---------------- |
| `dbsa.Format` | ✓ | ✓ | |
| `dbsa.Bucket` | ✓ | ✓ | |
| `dbsa.Sortkey` | | | ✓ |
| `dbsa.DistributionKey` | | | ✓ |

## PII data types for column classification

You can set up a `pii` object to describe how you wish to handle your PII information stored on HDFS or within Redshift.

```python
import dbsa

pii = dbsa.PII(
    EMAIL=dbsa.DataType(transform_on_insert="FUNC_SHA1({quoted_name})"),
    IP_ADDRESS=dbsa.DataType(drop_on=dbsa.PII.INSERT),
    DEVICE_ID=dbsa.DataType(),
)
``