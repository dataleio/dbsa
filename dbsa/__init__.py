import re
import copy
from bisect import bisect
from jinja2 import Template

"""
Markdown documentation variable
"""

MARKDOWN = """
{{ header }} {{ t.full_table_name(quoted=False, with_prefix=False) }}
{{ inspect.getdoc(t) or '' }}

| Column name | Column Type | PII | Description |
| ----------- | ---- | --- | ----------- |
{%- for c in t.columns() if c.attrs.get('keep', True) and c.pii.drop_on != 'INSERT' %}
| {% if c.partition %}**{% endif %}{{ c.name }}{% if c.partition %}**{% endif %} | `{{ c.column_type }}` | {{ c.pii.name or '' }} | {{ c.comment or '' }} |
{%- endfor %}
"""

"""
Error message collection that can be fired during schema
definitions.
"""

class ColumnAttributesMissing(AttributeError):
    pass

class PrototypeRequired(AttributeError):
    pass

class ColumnRequired(AttributeError):
    pass

class ColumnNameRequired(AttributeError):
    pass

class ColumnNameNotUnique(AttributeError):
    pass

class NotSupportedDialect(RuntimeError):
    pass


"""
Cleanup function for staging tables
"""

def cleanup_fn(value, quoted, dashed):
    rvalue = re.sub('^.*\((.*?)\)$', '\\1', str(value))
    if not quoted:
        rvalue = rvalue.replace("'", '')
    if not dashed:
        rvalue = rvalue.replace('{{ ds }}', '{{ ds_nodash }}') \
                       .replace('{{ ts }}', '{{ ts_nodash }}')
    return rvalue

"""
The following classes represents th
"""

class PII(object):
    INSERT = 'insert'
    DELETE = 'delete'

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        for k, v in kwargs.items():
            v.name = k


class DataType(object):
    def __init__(self, drop_on=None, transform_on_insert=None, transform_on_delete=None):
        self.drop_on = drop_on
        self.transform_on_delete = transform_on_delete
        self.transform_on_insert = transform_on_insert
        self.name = None

"""
Table policies
"""

class TablePolicy(object):
    def resolve(self, dialect):
        raise NotImplemented('TablePolicy.resolve is not implemented')


class PartitionRetentionPolicy(TablePolicy):
    def __init__(self, ds_ago, earliest_partition=None):
        self.earliest_partition = earliest_partition
        self.ds_ago = ds_ago

    def table(self, dialect):
        if not self.earliest_partition:
            raise RuntimeError('PartitionRetentionPolicy.table() is not supported without earliest_partition specified')
        return dialect.clone(**self.earliest_partition)

    def resolve(self, dialect):
        tbl = self.table(dialect)
        return tbl.get_delete_current_partition(
            ignored_partitions=set(tbl.partition_names()) - set(self.earliest_partition.keys())
        )


class PartitionAnonimisationPolicy(TablePolicy):
    def __init__(self, ds_ago, earliest_partition=None):
        self.earliest_partition = earliest_partition
        self.ds_ago = ds_ago

    def table(self, dialect):
        if not self.earliest_partition:
            raise RuntimeError('AnonomisationPolicy.table() is not supported without earliest_partition specified')
        return dialect.clone(**self.earliest_partition)


class ManualAnonimisation(TablePolicy):
    pass


"""
Generic objects that are associated to Tables. It can be a property of the table
or a column itself.
"""

class TableProperty(object):
    _property_type = None
    _req_properties = None

    def __init__(self, **kwargs):
        self.attrs = kwargs or {}

    def __str__(self):
        if not self._property_type:
            raise NotImplemented('Column._property_type is not defined or __str__ method is not implemented')

        return Template(self._property_type).render(**{ k: v for k,v in self.attrs.items() })

    def register_dialect(self, dialect):
        self._req_properties = dialect._req_properties.get(self.__class__)
        self._property_type = dialect._property_types.get(self.__class__)
        if not p._property_type: raise NotSupportedDialect

        if not (set(self._req_properties or []) <= set((self.attrs or {}).keys())):
            raise ColumnAttributesMissing('{} - following attributes are required: {}'.format(self.name, self._req_properties))

class Column(object):
    _creation_counter = 0
    _column_type = None
    _req_properties = None
    _how_to_quote = '"{}"'
    _column_setter = '{} AS {}'

    def __init__(self, name=None, pii=None, comment=None, **kwargs):
        # Store base column values
        self.name = name
        self.value = None
        self.partition = False
        self.pii = pii or DataType()
        self.attrs = kwargs or {}
        self.comment = comment
        self.manually_set = False

        # Set up Creation Counter to track number of columns and its order
        self._creation_counter = Column._creation_counter
        Column._creation_counter += 1

    def __cmp__(self, other):
        return cmp(self._creation_counter, other._creation_counter)

    def __lt__(self, other):
        return self._creation_counter < other._creation_counter

    def set_column_value(self, value):
        self.value = value
        self.manually_set = True

    @property
    def quoted_name(self):
        return self._how_to_quote.format(self.name)

    @property
    def default_load_value(self):
        if self.manually_set:
            return self._column_setter.format(self.value or self.quoted_name, self.quoted_name)

        if self.pii.drop_on != PII.INSERT and self.pii.transform_on_insert is None:
            return self.quoted_name

        if self.pii.drop_on == PII.INSERT:
            return self._column_setter.format('NULL', self.quoted_name)

        if self.pii.transform_on_insert is not None:
            return self._column_setter.format(self.pii.transform_on_insert.format(quoted_name=self.quoted_name), self.quoted_name)

    @property
    def column_type(self):
        if not self._column_type:
            raise NotImplemented('Column._column_type is not defined or __str__ method is not implemented')

        return Template(self._column_type).render(**self.__dict__)

    def register_dialect(self, dialect):
        self._how_to_quote = dialect._how_to_quote_column
        self._column_setter = dialect._column_setter
        self._req_properties = dialect._req_properties.get(self.__class__, self._req_properties)
        self._column_type = dialect._column_types.get(self.__class__, self._column_type)
        if not self._column_type: raise NotSupportedDialect

        if not (set(self._req_properties or []) <= set((self.attrs or {}).keys())):
            raise ColumnAttributesMissing('{} - following attributes are required: {}'.format(self.name, self._req_properties))

        for rp in set(self._req_properties or []):
            setattr(self, rp, self.attrs[rp])


class Partition(Column):
    _column_type = '{{ column.column_type }}'
    def __init__(self, column, value=None, name=None, **kwargs):
        super(Partition, self).__init__(**kwargs)
        self.value = value
        self.column = column
        self.partition = True

    @property
    def default_load_value(self):
        return self._column_setter.format(self.value or self.quoted_name, self.quoted_name)

    def register_dialect(self, dialect):
        super(Partition, self).register_dialect(dialect)
        self.column.register_dialect(dialect)

        for rp in set(self.column._req_properties or []):
            setattr(self, rp, self.column.attrs[rp])

# Default base table properties for schema matching between dialects

class Format(TableProperty):
    pass

class Bucket(TableProperty):
    pass

class Sortkey(TableProperty):
    pass

class DistributionKey(TableProperty):
    pass

# Default base column types for schema matching between dialects

class Boolean(Column):
    pass

class Tinyint(Column):
    pass

class Smallint(Column):
    pass

class Integer(Column):
    pass

class Bigint(Column):
    pass

class Real(Column):
    pass

class Double(Column):
    pass

class Decimal(Column):
    pass

class Varchar(Column):
    pass

class Char(Column):
    pass

class Varbinary(Column):
    pass

class JSON(Column):
    pass

class Date(Column):
    pass

class Time(Column):
    pass

class Timestamp(Column):
    pass

class Array(Column):
    _req_properties = {'data_type'}
    def register_dialect(self, dialect):
        super(Array, self).register_dialect(dialect)
        self.data_type.register_dialect(dialect)

class Map(Column):
    _req_properties = {'primitive_type', 'data_type'}
    def register_dialect(self, dialect):
        super(Map, self).register_dialect(dialect)
        self.primitive_type.register_dialect(dialect)
        self.data_type.register_dialect(dialect)

class Row(Column):
    _req_properties = {'columns'}
    def register_dialect(self, dialect):
        super(Row, self).register_dialect(dialect)
        for c in self.columns:
            c.register_dialect(dialect)

class IPAddress(Column):
    pass


# Class registers that counts all occurances and validates column existance

class Prototype(object):
    def __init__(self, columns, props, policies):
        if not len(columns):
            raise ColumnRequired('Prototype requires at least one Column!')

        known_column_names = set()
        for column in columns:
            if not column.name:
                raise ColumnNameRequired("Column's name attribute is required!")

            if column.name in known_column_names:
                raise ColumnNameNotUnique("Field's name must be unique!")

            known_column_names.add(column.name)

        self.columns = columns
        self.props = props
        self.policies = policies


class PrototypeGenerator(type):
    def __new__(metacls, name, bases, namespace, **kwds):
        cls = super(PrototypeGenerator, metacls).__new__(metacls, name, bases, dict(namespace))
        columns, props, policies = [], [], []
        for name, obj in namespace.items():
            if isinstance(obj, Column):
                obj.name = name
                columns.insert(bisect(columns, obj), obj)
            if isinstance(obj, TableProperty):
                props.append(obj)
            if isinstance(obj, TablePolicy):
                policies.append(obj)

        if len(columns):
            cls._prototype = Prototype(columns, props, policies)

        return cls

# Python 2 & 3 metaclass decorator from `six` package.
def add_metaclass(metaclass):
    def wrapper(cls):
        orig_vars = cls.__dict__.copy()
        slots = orig_vars.get('__slots__')
        if slots is not None:
            if isinstance(slots, str):
                slots = [slots]
            for slots_var in slots:
                orig_vars.pop(slots_var)
        orig_vars.pop('__dict__', None)
        orig_vars.pop('__weakref__', None)
        return metaclass(cls.__name__, cls.__bases__, orig_vars)
    return wrapper


@add_metaclass(PrototypeGenerator)
class Table(object):
    table_prefix = ''
    _how_to_quote = '"{}"'

    def __init__(self, schema, dialect=None, **values):
        if not hasattr(self, '_prototype'):
            raise PrototypeRequired('Prototype declaration is required!')

        self._columns = []
        for column in self._prototype.columns:
            setattr(self, column.name, copy.copy(column))
            self._columns.append(getattr(self, column.name))
            if column.name in values.keys():
                getattr(self, column.name).value = values[column.name]

        self._props = copy.copy(self._prototype.props)
        self._policies = {p.__class__.__name__ : p for p in self._prototype.policies}

        self.schema = schema
        self.dialect = None
        self.register_dialect(dialect)

    @property
    def partitions(self):
        return [c for c in self._columns if c.partition]

    @property
    def properties(self):
        return self._props

    @property
    def table_name(self):
        return re.sub('(?!^)([A-Z]+)', r'_\1', self.__class__.__name__).lower()

    @property
    def table_name_with_prefix(self):
        return self.table_prefix + self.table_name

    def partition_definition(self, cleanup_fn=cleanup_fn):
        return '/'.join('{name}={value}'.format(
            name=p.name,
            value=cleanup_fn(p.value, quoted=False, dashed=True),
        ) for p in self.partitions if p.value is not None)

    def staging_table_name(self, cleanup_fn=cleanup_fn):
        named_partitions = '_'.join(cleanup_fn(c.value, quoted=False, dashed=False) for c in self.partitions if c.value)
        if not named_partitions: return 'stg_' + self.table_name
        return 'stg_' + '_'.join([named_partitions, self.table_name])

    def staging_table_name_with_prefix(self, cleanup_fn=cleanup_fn):
        return self.table_prefix + self.staging_table_name(cleanup_fn)

    def register_dialect(self, dialect):
        if dialect is None: return

        for c in self._columns:
            c.register_dialect(dialect)

        for p in self._props:
            p._req_properties = dialect._req_properties.get(p.__class__)
            p._property_type = dialect._property_types.get(p.__class__)
            if not p._property_type: raise NotSupportedDialect

        self._how_to_quote = dialect._how_to_quote_table
        self.dialect = dialect

    def _quote(self, text, quoted):
        return text if not quoted else self._how_to_quote.format(text)

    def columns(self, include_partitions=True, filter_fn=None):
        columns = self._columns if not filter_fn else filter(filter_fn, self._columns)
        return [c for c in columns if (not c.partition) or include_partitions]

    def column_names(self, include_partitions=True, filter_fn=None):
        return {c.name for c in self.columns(include_partitions=include_partitions, filter_fn=filter_fn)}

    def partition_names(self):
        return {p.name for p in self.partitions}

    def full_table_name(self, quoted=False, with_prefix=False, suffix=''):
        table_name = self._quote((self.table_name_with_prefix if with_prefix else self.table_name) + suffix, quoted)
        schema = self._quote(self.schema, quoted)
        return '{}.{}'.format(schema, table_name)

    def full_staging_table_name(self, cleanup_fn=cleanup_fn, quoted=False, with_prefix=False, suffix=''):
        table_name = self._quote((self.staging_table_name_with_prefix(cleanup_fn=cleanup_fn) if with_prefix else self.staging_table_name(cleanup_fn=cleanup_fn)) + suffix, quoted)
        schema = self._quote(self.schema, quoted)
        return '{}.{}'.format(schema, table_name)

    def column_values(self, include_partitions=True, filter_fn=None):
        return (c.default_load_value for c in self.columns(include_partitions=include_partitions, filter_fn=filter_fn))

    def get_current_partition_params(self, params=None):
        _params = {c.name: c.value for c in self.partitions if c.value is not None}
        _params.update(params or {})
        return _params

    def get_current_partition_condition(self, condition='', ignored_partitions=None, sep=' AND '):
        partition_names = {p.name for p in self.partitions} - set(ignored_partitions or [])
        partitions = [p for p in self.partitions if p.name in partition_names]
        conditions = ['{quoted_name} = {{{name}}}'.format(name=p.name, quoted_name=p.quoted_name) for p in partitions]
        if condition: conditions.append(condition)
        return sep.join(conditions)


class Dialect(object):
    _column_types = {}
    _req_properties = {}
    _property_types = {}
    _how_to_quote_table = '"{}"'
    _how_to_quote_column = '"{}"'
    _column_setter = '{} AS {}'
    _exposed_table_functions = [
        'partitions',
        'properties',
        'table_name',
        'table_name_with_prefix',
        'partition_definition',
        'staging_table_name',
        'staging_table_name_with_prefix',
        'columns',
        'column_names',
        'partition_names',
        'full_table_name',
        'full_staging_table_name',
        'column_values',
        'get_current_partition_params',
        'get_current_partition_condition',
    ]

    def __init__(self, table):
        self.table = table
        self.table.register_dialect(self)
        for fn in self._exposed_table_functions:
            if not hasattr(self, fn):
                setattr(self, fn, getattr(self.table, fn))

    def add_table_column(self, column):
        self.table._columns.append(column)
        setattr(self.table, column.name, column)
        column.register_dialect(self)

    def to_markdown(self, header='###'):
        import inspect
        return Template(MARKDOWN).render(t=self.table, inspect=inspect, header=header)

    def clone(self, **kwargs):
        return self.__class__(self.table.__class__(
            schema=self.table.schema,
            **kwargs,
        ))

    def lookup_policy(self, type_cls):
        return self.table._policies.get(type_cls.__name__)

    def resolve_policy(self, type_cls):
        lookup = self.lookup_policy(type_cls.__name__)
        if not lookup:
            return

        return lookup.resolve(self)

    def get_create_table(self, filter_fn=None, suffix=''):
        raise NotImplemented()

    def get_drop_table(self, suffix=''):
        raise NotImplemented()

    def get_truncate_table(self, suffix=''):
        raise NotImplemented()

    def get_select(self, filter_fn=None, suffix='', condition=''):
        raise NotImplemented()

    def get_select_current_partition(self, filter_fn=None, condition='', params=None, ignored_partitions=None, suffix=''):
        return self.get_select(
            filter_fn=filter_fn,
            suffix=suffix,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions) \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_delete_current_partition(self, condition='', params=None, ignored_partitions=None, suffix=''):
        return self.get_delete_from(
            condition=self.table.get_current_partition_condition(condition, ignored_partitions),
            params=self.table.get_current_partition_params(params),
            suffix=suffix,
        )

    def get_delete_from(self, condition=None, params=None, suffix=''):
        raise NotImplemented()

    def get_insert_into_from_table(self, source_table_name, filter_fn=None, suffix=''):
        raise NotImplemented()

    def get_insert_into_via_select(self, select, filter_fn=None, embed_select=True, suffix=''):
        raise NotImplemented()

    def get_drop_current_partition_view(self, suffix='_latest'):
        raise NotImplemented()

    def get_create_current_partition_view(self, suffix='_latest', condition='', ignored_partitions=None, params=None):
        raise NotImplemented()
