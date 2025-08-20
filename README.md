# dbsa

Database schemas for Airflow. One of the biggest issue with [Apache Airflow](https://airflow.apache.org/index.html) that it does not provide any good way to describe the database schemas within the system. It leads incorrect table definitions, hard to extend schemas and keep backwards compatibility of already existing pipelines.

This package was written in mind to **use as the abstraction layer of table schemas**, and it provides support for [Presto](http://prestodb.github.io/), [Trino](https://trino.io/), [Apache Hive](https://hive.apache.org/), and [Amazon Redshift](https://aws.amazon.com/redshift/). Additionally, it includes specialized support for [Trino Iceberg](https://trino.io/docs/current/connector/iceberg.html) tables.

## Installation

Installation is as simple as installing any other [Python packages](https://www.python.org/).

```bash
$ pip install dbsa
```

## Supported column types

The following column types are supported:

| Date type        | Presto support | Trino support | Trino Iceberg support | Hive support   | Redshift support |
| ---------------- | -------------- | ------------- | --------------------- | -------------- | ---------------- |
| `dbsa.Boolean`   | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Tinyint`   | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Smallint`  | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Integer`   | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Bigint`    | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Real`      | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Double`    | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Decimal`   | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Varchar`   | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Char`      | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Varbinary` | ✓              | ✓             | ✓                     | ✓              |                  |
| `dbsa.JSON`      | ✓              | ✓             | ✓                     | ✓ AS `Varchar` |                  |
| `dbsa.Date`      | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Time`      | ✓              | ✓             | ✓                     |                |                  |
| `dbsa.Timestamp` | ✓              | ✓             | ✓                     | ✓              | ✓                |
| `dbsa.Array`     | ✓              | ✓             | ✓                     | ✓              |                  |
| `dbsa.Map`       | ✓              | ✓             | ✓                     | ✓              |                  |
| `dbsa.Row`       | ✓              | ✓             | ✓                     | ✓              |                  |
| `dbsa.IPAddress` | ✓              | ✓             | ✓                     | ✓ AS `Varchar` |                  |


## Supported Table Properties

The following table properties are supported:

| Date type                | Presto support | Trino support | Trino Iceberg support | Hive support | Redshift support |
| ------------------------ | -------------- | ------------- | --------------------- | ------------ | ---------------- |
| `dbsa.Format`            | ✓              | ✓             | ✓                     | ✓            |                  |
| `dbsa.Bucket`            | ✓              | ✓             | ✓                     | ✓            |                  |
| `dbsa.Sortkey`           |                |               |                       |              | ✓                |
| `dbsa.DistributionKey`   |                |               |                       |              | ✓                |
| `dbsa.DistributionStyle` |                |               |                       |              | ✓                |

## PII data types for column classification

You can set up a `pii` object to describe how you wish to handle your PII information stored on HDFS or within Redshift.

```python
import dbsa

pii = dbsa.PII(
    EMAIL=dbsa.DataType(transform_on_insert="FUNC_SHA1({quoted_name} || CAST(created_at AS VARCHAR))"),
    IP_ADDRESS=dbsa.DataType(drop_on=dbsa.PII.DELETE),
    DEVICE_ID=dbsa.DataType(),
)
```

When you perform an `INSERT` statement, the transformations will be done automatically, and if column drop was specified on INSERT, the values will be truncated.

If you set up `transform_on_delete` or drop on `DELETE` conditions, you must write a pipeline to specify when the condition met.

## Describing tables

I recommend to create seperate files for each schemas and namespaces in the `airflow/schemas` folder. You can describe a table with the following way:

```python
import dbsa

class Metrics(dbsa.Table):
    """
    You can add your table defintion here, which will show up in the documentation automatically.
    It helps building a data dictionary of tables and columns, and also document your codebase.
    """
    _format = dbsa.Format(format='ORC')
    ds = dbsa.Partition(dbsa.Varchar(), comment="Date of the metrics are beging generated.")
    aggregation = dbsa.Partition(dbsa.Varchar(), comment="Name of the aggregation. All metrics within an aggregation are populated at the same time - however aggregations can land at different times!")
    metric = dbsa.Varchar(comment='Name of a standalone metric. (e.g: visits)')
    dimensions = dbsa.Map(primitive_type=dbsa.Varchar(), data_type=dbsa.Varchar(), comment='Dimensions are used for the calculations')
    grouping_id = dbsa.Bigint(comment='Unique grouping identifier of the selected dimensions.')
    value = dbsa.Double(comment='Value of the metric.')
    proportion = dbsa.Double(comment='Proportion of the metric and the total value if it is applicable.')
    total = dbsa.Double(comment='Total value if it is applicable.')
```

This table definition is not binded to any dialect yet. To use the table, you must bind it to one. When creating the table instances, we must specify the name of the `schema`, and fill the missing partitions. `dbsa` will not quote your data since you can use functions, UDFs, so please put quotes around your data if it's needed.

```python
from dbsa import presto, hive, trino, trino_iceberg

presto_tbl = presto.Table(Metrics(schema='default', ds="'2019-07-27'"))
hive_tbl = hive.Table(Metrics(schema='default', ds="'2019-07-27'"))
trino_tbl = trino.Table(Metrics(schema='default', ds="'2019-07-27'"))
trino_iceberg_tbl = trino_iceberg.Table(Metrics(schema='default', ds="'2019-07-27'"))
```

After that, we can start generating SQL queries based on the dialect.

```python
print(presto_tbl.get_delete_current_partition(ignored_partitions=['aggregation']))
# DELETE FROM "default"."metrics"
# WHERE "ds" = '2019-07-27'

print(hive_tbl.get_delete_current_partition(ignored_partitions=['aggregation']))
# ALTER TABLE `default`.`metrics` DROP IF EXISTS PARTITION(
#  `ds` = '2019-07-27'
# ) PURGE
```
As you can see, the dialects are working quite easily, and we can even specify that ignore our `aggregation` subpartition.

## Trino and Trino Iceberg Support

The package now includes support for both **Trino** and **Trino Iceberg** dialects:

- **`dbsa.trino`**: Standard Trino (on Hive) support with `partitioned_by` and `external_location` properties
- **`dbsa.trino_iceberg`**: Specialized support for Iceberg tables using `partitioning` and `location` properties

### Trino Iceberg Features

Trino Iceberg support includes:

- **Custom partitions**: Add additional partitions beyond the table's defined partitions
- **Iceberg-specific properties**: Uses `location` instead of `external_location` and `partitioning` instead of `partitioned_by`

```python
from dbsa import trino_iceberg

# Create Iceberg table with additional custom partitions
iceberg_tbl = trino_iceberg.Table(
    Metrics(schema='default', ds="'2019-07-27'"),
    custom_partitions=['region', 'category']
)

# External table properties for Iceberg
iceberg_props = trino_iceberg.ExternalTableProperties(
    location='s3://my-bucket/data/',
    configs={'table_type': 'ICEBERG'}
)
```

```python
print(presto_tbl.get_create_table())
# CREATE TABLE IF NOT EXISTS "default"."metrics" (
#   "metric" VARCHAR COMMENT 'Name of a standalone metric. (e.g: visits)',
#   "dimensions" MAP(VARCHAR, VARCHAR) COMMENT 'Dimensions are used for the calculations',
#   "grouping_id" BIGINT COMMENT 'Unique grouping identifier of the selected dimensions.',
#   "value" DOUBLE COMMENT 'Value of the metric.',
#   "proportion" DOUBLE COMMENT 'Proportion of the metric and the total value if it is applicable.',
#   "total" DOUBLE COMMENT 'Total value if it is applicable.',
#   "ds" VARCHAR COMMENT 'Date of the metrics are beging generated.',
#   "aggregation" VARCHAR COMMENT 'Name of the aggregation. All metrics within an aggregation are populated at the same time - however aggregations can land at different times!'
# )
# COMMENT 'You can add your table defintion here, which will show up in the documentation automatically.
# It helps building a data dictionary of tables and columns, and also document your codebase.'
# WITH (
#   partitioned_by = ARRAY[
#     'ds',
#     'aggregation'
#   ],
#   format = 'ORC'
# )
```

## Adding Policies to tables

We support `PartitionRetentionPolicy` to set up retentions for your tables. These policies are not enforced however, you must write an Airflow pipeline to drop the old partitions.

```python
class IncomingEvents(dbsa.Table):
    """
    Good example how you can set up a 30 days retention for one of your tables. Also, you can see how
    you can set up PII classification for your data.
    """
    _retention = dbsa.PartitionRetentionPolicy(earliest_partition={'ds': "'{{ macros.ds_add(ds, -30) }}'"})
    email = dbsa.Varchar(comment='Email address marked as PII', pii=pii.EMAIL)
```

## Generate a documentation

You must pick a dialect, and just run the following command.

```bash
dbsa-markdown {presto|trino|trino_iceberg|hive|redshift} file.py
```
