import re
import copy
import sys
from bisect import bisect
from typing import Generic, TypeVar, get_args, get_origin

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

class BareColumnRequiresConcreteAnnotation(TypeError):
    pass

class NotSupportedDialect(RuntimeError):
    pass


"""
Cleanup function for staging tables
"""

def cleanup_fn(value, quoted, dashed):
    """Normalize partition values for paths or staging names (quotes, ds/ts placeholders)."""
    rvalue = re.sub(r'^.*\((.*?)\)$', r'\1', str(value))
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
    """Named bundle of :class:`DataType` entries for PII handling (e.g. ``EMAIL=...``)."""

    INSERT = 'insert'
    DELETE = 'delete'

    def __init__(self, **kwargs):
        """Register each keyword as a :class:`DataType` with ``name`` set to the key."""
        self.__dict__.update(kwargs)
        for k, v in kwargs.items():
            v.name = k


class DataType(object):
    """Per-column PII behaviour (drop/transform on insert or delete)."""

    def __init__(self, drop_on=None, transform_on_insert=None, transform_on_delete=None):
        """Store optional PII lifecycle hooks for a column."""
        self.drop_on = drop_on
        self.transform_on_delete = transform_on_delete
        self.transform_on_insert = transform_on_insert
        self.name = None

"""
Table policies
"""

class TablePolicy(object):
    """Optional table-level behaviour resolved against a bound dialect."""

    def resolve(self, dialect):
        """Return SQL or other artefact for this policy; subclasses must implement."""
        raise NotImplemented('TablePolicy.resolve is not implemented')


class PartitionRetentionPolicy(TablePolicy):
    """Describe which partitions to keep; resolved to a DELETE (or similar) statement."""

    def __init__(self, ds_ago, earliest_partition=None):
        """``earliest_partition`` maps partition column names to literal values for the clone."""
        self.earliest_partition = earliest_partition
        self.ds_ago = ds_ago

    def table(self, dialect):
        """Return a dialect table instance with partition values from ``earliest_partition``."""
        if not self.earliest_partition:
            raise RuntimeError('PartitionRetentionPolicy.table() is not supported without earliest_partition specified')
        return dialect.clone(**self.earliest_partition)

    def resolve(self, dialect):
        """SQL to delete partitions not listed in ``earliest_partition``."""
        tbl = self.table(dialect)
        return tbl.get_delete_current_partition(
            ignored_partitions=set(tbl.partition_names()) - set(self.earliest_partition.keys())
        )


class PartitionAnonimisationPolicy(TablePolicy):
    """Placeholder policy for partition anonymisation pipelines."""

    def __init__(self, ds_ago, earliest_partition=None):
        self.earliest_partition = earliest_partition
        self.ds_ago = ds_ago

    def table(self, dialect):
        """Clone the dialect table with ``earliest_partition`` kwargs."""
        if not self.earliest_partition:
            raise RuntimeError('AnonomisationPolicy.table() is not supported without earliest_partition specified')
        return dialect.clone(**self.earliest_partition)


class ManualAnonimisation(TablePolicy):
    """Marker policy for manual anonymisation flows."""
    pass


"""
Generic objects that are associated to Table. It can be a property of various process in a Table.
"""

class ExternalTableProperties(object):
    """Base for dialect-specific external table location + key/value properties."""

    def __init__(self, location, configs=None):
        self.location = location
        self.configs = configs or {}

    def get_properies(self):
        """Return a list of ``WITH`` clause property strings; subclasses must implement."""
        raise NotImplemented()

"""
Generic objects that are associated to Tables. It can be a property of the table
or a column itself.
"""

class TableProperty(object):
    """Table-level option (format, bucket, sortkey, …) rendered for a dialect."""

    _property_type = None
    _req_properties = None

    def __init__(self, **kwargs):
        """Store template fields in ``attrs``."""
        self.attrs = kwargs or {}

    def __str__(self):
        """Render this property using the dialect Jinja template."""
        if not self._property_type:
            raise NotImplemented('Column._property_type is not defined or __str__ method is not implemented')

        return Template(self._property_type).render(**{ k: v for k,v in self.attrs.items() })

    def register_dialect(self, dialect):
        """Bind dialect templates and validate required ``attrs`` keys."""
        self._req_properties = dialect._req_properties.get(self.__class__)
        self._property_type = dialect._property_types.get(self.__class__)
        if not self._property_type: raise NotSupportedDialect

        if not (set(self._req_properties or []) <= set((self.attrs or {}).keys())):
            raise ColumnAttributesMissing('{} - following attributes are required: {}'.format(self.name, self._req_properties))

class Column(object):
    """Logical column or type node; subclass for each SQL type."""

    _creation_counter = 0
    _column_type = None
    _req_properties = None
    _how_to_quote = '"{}"'
    _column_setter = '{} AS {}'

    def __init__(self, name=None, pii=None, comment=None, default_value=None, **kwargs):
        """``kwargs`` become ``attrs`` (length, precision, nested types, …)."""
        # Store base column values
        self.name = name
        self.value = None
        self.default_value = default_value
        self.partition = False
        self.pii = pii or DataType()
        self.attrs = kwargs or {}
        self.comment = comment
        self.manually_set = False

        # Set up Creation Counter to track number of columns and its order
        self._creation_counter = Column._creation_counter
        Column._creation_counter += 1

    def __cmp__(self, other):
        """Python 2 ordering by declaration order (legacy)."""
        return cmp(self._creation_counter, other._creation_counter)

    def __lt__(self, other):
        """Order columns by declaration order for stable sorting."""
        return self._creation_counter < other._creation_counter

    def set_column_value(self, value):
        """Mark this column as supplied explicitly (LOAD / UPDATE paths)."""
        self.value = value
        self.manually_set = True

    @property
    def quoted_name(self):
        """Dialect-quoted identifier for this column."""
        return self._how_to_quote.format(self.name)

    @property
    def default_load_value(self):
        """Expression used in SELECT lists for INSERT (PII / manual overrides)."""
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
        """Rendered SQL type for this column in the active dialect."""
        if not self._column_type:
            raise NotImplemented('Column._column_type is not defined or __str__ method is not implemented')

        return Template(self._column_type).render(**self.__dict__)

    def register_dialect(self, dialect):
        """Wire quoting, templates, and required attributes for ``dialect``."""
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
    """Hive-style partition column wrapping an inner :class:`Column` type."""

    _column_type = '{{ column.column_type }}'
    def __init__(self, column, value=None, name=None, **kwargs):
        """``column`` is the storage type; optional ``value`` fixes a partition literal."""
        super(Partition, self).__init__(**kwargs)
        self.value = value
        self.column = column
        self.partition = True

    @property
    def default_load_value(self):
        """Partition columns always map from value (or name) to quoted name."""
        return self._column_setter.format(self.value or self.quoted_name, self.quoted_name)

    def register_dialect(self, dialect):
        """Register both the partition shell and the inner column for ``dialect``."""
        super(Partition, self).register_dialect(dialect)
        self.column.register_dialect(dialect)

        for rp in set(self.column._req_properties or []):
            setattr(self, rp, self.column.attrs[rp])

# Default base table properties for schema matching between dialects


class Format(TableProperty):
    """Table file format (ORC, Parquet, …) for dialect-specific DDL."""

    pass


class Bucket(TableProperty):
    """Hive/Presto-style bucketing specification."""

    pass


class Sortkey(TableProperty):
    """Redshift sort key column list."""

    pass


class DistributionKey(TableProperty):
    """Redshift distribution key column."""

    pass


class DistributionStyle(TableProperty):
    """Redshift ``DISTSTYLE`` clause."""

    pass


# Default base column types for schema matching between dialects


class Boolean(Column):
    """SQL ``BOOLEAN``."""

    pass


class Tinyint(Column):
    """SQL ``TINYINT`` / one-byte integer."""

    pass


class Smallint(Column):
    """SQL ``SMALLINT``."""

    pass


class Integer(Column):
    """SQL ``INTEGER`` / ``INT`` (dialect-specific spelling)."""

    pass


class Bigint(Column):
    """SQL ``BIGINT``."""

    pass


class Real(Column):
    """SQL ``REAL`` / single-precision float."""

    pass


class Double(Column):
    """SQL ``DOUBLE`` / double-precision float."""

    pass


class Decimal(Column):
    """SQL ``DECIMAL`` / ``NUMERIC`` with ``precision`` and ``scale``."""

    pass


class Varchar(Column):
    """SQL variable-length string (optional ``length``)."""

    pass


class Char(Column):
    """SQL fixed-length ``CHAR`` (requires ``length`` where enforced)."""

    pass


class Varbinary(Column):
    """SQL binary type (requires ``length`` where enforced)."""

    pass


class JSON(Column):
    """JSON logical type (dialects may map to VARCHAR or native JSON)."""

    pass


class Date(Column):
    """SQL ``DATE``."""

    pass


class Time(Column):
    """SQL ``TIME``."""

    pass


class Timestamp(Column):
    """SQL ``TIMESTAMP`` (optional precision and time zone)."""

    pass


_ArrayElement = TypeVar("_ArrayElement", bound=Column)


class Array(Column, Generic[_ArrayElement]):
    """SQL array type with ``data_type`` element schema.

    Prefer ``name: Array[Integer] = Column(...)`` so the element type is visible in the
    annotation; ``name: Array = Column(data_type=Integer())`` remains supported.
    """

    _req_properties = {'data_type'}
    def register_dialect(self, dialect):
        """Register element type recursively."""
        super(Array, self).register_dialect(dialect)
        self.data_type.register_dialect(dialect)

class Map(Column):
    """SQL map type with key and value column schemas.

    Prefer ``name: Map[Varchar, Boolean] = Column(...)`` (key type, value type). You may pass
    column instances in the brackets, e.g. ``Map[Varchar(length=4), Boolean()]``. The form
    ``name: Map = Column(primitive_type=..., data_type=...)`` remains supported.
    """

    _req_properties = {"primitive_type", "data_type"}

    @classmethod
    def __class_getitem__(cls, params):
        if not isinstance(params, tuple):
            params = (params,)
        if len(params) != 2:
            raise TypeError(
                "Map[...] expects two parameters: primitive_type (key) and data_type (value), "
                "for example dbsa.Map[dbsa.Varchar, dbsa.Boolean]."
            )
        try:
            from types import GenericAlias
        except ImportError:  # pragma: no cover
            raise TypeError("Map[...] subscripting requires Python 3.9+.")
        return GenericAlias(cls, params)

    def register_dialect(self, dialect):
        """Register key and value types recursively."""
        super(Map, self).register_dialect(dialect)
        self.primitive_type.register_dialect(dialect)
        self.data_type.register_dialect(dialect)

class Row(Column):
    """SQL row/struct type built from nested :class:`Column` instances."""

    _req_properties = {'columns'}
    def register_dialect(self, dialect):
        """Register each field column recursively."""
        super(Row, self).register_dialect(dialect)
        for c in self.columns:
            c.register_dialect(dialect)

class IPAddress(Column):
    """IP address logical type (dialects may map to VARCHAR, etc.)."""
    pass


# Class registers that counts all occurances and validates column existance

class Prototype(object):
    """Validated snapshot of columns, table properties, and policies for a :class:`Table` class."""

    def __init__(self, columns, props, policies):
        """Ensure at least one column and unique names."""
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


def _nested_column_from_type_arg(arg):
    """Build a :class:`Column` instance from a bracket annotation (class or instance)."""
    if isinstance(arg, Column):
        return copy.copy(arg)
    if isinstance(arg, type) and issubclass(arg, Column):
        return arg()
    raise BareColumnRequiresConcreteAnnotation(
        "Nested column type must be a dbsa column class or instance (got {!r}).".format(arg)
    )


def _unpack_parameterized_array(column_type):
    """If ``column_type`` is ``Array[SomeColumn]``, return the inner type or instance; else ``None``."""
    origin = get_origin(column_type)
    if origin is not Array:
        return None
    args = get_args(column_type)
    if len(args) != 1:
        raise BareColumnRequiresConcreteAnnotation(
            "Array[...] requires exactly one element type, for example c: dbsa.Array[dbsa.Integer] = Column(...)."
        )
    return args[0]


def _unpack_parameterized_map(column_type):
    """If ``column_type`` is ``Map[key, value]``, return ``(key_arg, value_arg)``; else ``None``."""
    origin = get_origin(column_type)
    if origin is not Map:
        return None
    args = get_args(column_type)
    if len(args) != 2:
        raise BareColumnRequiresConcreteAnnotation(
            "Map[...] requires key and value types, for example c: dbsa.Map[dbsa.Varchar, dbsa.Boolean] = Column(...)."
        )
    return args[0], args[1]


def _resolve_class_annotation(annotation, namespace):
    """Resolve annotation values for table columns (including string annotations)."""
    if isinstance(annotation, str):
        module_name = namespace.get('__module__')
        eval_globals = {'__builtins__': __builtins__}
        if module_name and sys.modules.get(module_name):
            eval_globals.update(sys.modules[module_name].__dict__)
        eval_globals.update(namespace)
        # ``from __future__ import annotations`` yields ``'dbsa.Bigint'``; the class body
        # namespace does not include the module object, so ensure ``dbsa`` resolves.
        pkg = sys.modules.get(__name__)
        if pkg is not None:
            eval_globals.setdefault('dbsa', pkg)
        return eval(annotation, eval_globals)
    return annotation


def _column_from_bare_and_annotation(column_type, bare_column):
    """Build a typed column from ``name: Bigint = Column(...)`` style declarations."""
    array_element = _unpack_parameterized_array(column_type)
    if array_element is not None:
        attrs = dict(bare_column.attrs or {})
        attrs["data_type"] = _nested_column_from_type_arg(array_element)
        return Array(
            pii=bare_column.pii,
            comment=bare_column.comment,
            default_value=bare_column.default_value,
            **attrs,
        )
    map_params = _unpack_parameterized_map(column_type)
    if map_params is not None:
        primitive_arg, data_type_arg = map_params
        attrs = dict(bare_column.attrs or {})
        if "primitive_type" not in attrs:
            attrs["primitive_type"] = _nested_column_from_type_arg(primitive_arg)
        if "data_type" not in attrs:
            attrs["data_type"] = _nested_column_from_type_arg(data_type_arg)
        return Map(
            pii=bare_column.pii,
            comment=bare_column.comment,
            default_value=bare_column.default_value,
            **attrs,
        )
    if column_type is Column:
        raise BareColumnRequiresConcreteAnnotation(
            'Use a concrete column type in the annotation (for example grouping_id: dbsa.Bigint = Column(...)).'
        )
    if not isinstance(column_type, type) or not issubclass(column_type, Column):
        raise BareColumnRequiresConcreteAnnotation(
            'Table column annotations must be dbsa column types (got {!r}).'.format(column_type)
        )
    if column_type is Partition:
        raise BareColumnRequiresConcreteAnnotation(
            'Partition columns must use ds = Partition(inner_type, ...) instead of annotation defaults.'
        )
    return column_type(
        pii=bare_column.pii,
        comment=bare_column.comment,
        default_value=bare_column.default_value,
        **(bare_column.attrs or {})
    )


def _promote_bare_columns_from_annotations(namespace, merged_annotations):
    """Replace bare ``Column(...)`` defaults with concrete types from ``__annotations__``."""
    for field_name, annotation in merged_annotations.items():
        if field_name not in namespace:
            continue
        candidate = namespace[field_name]
        if not isinstance(candidate, Column) or type(candidate) is not Column:
            continue
        resolved = _resolve_class_annotation(annotation, namespace)
        namespace[field_name] = _column_from_bare_and_annotation(resolved, candidate)


def _merge_class_annotations(bases, namespace):
    """Merge ``__annotations__`` from bases and the current class body (MRO order)."""
    merged = {}
    for base in bases:
        merged.update(getattr(base, '__annotations__', {}) or {})
    merged.update(namespace.get('__annotations__', {}) or {})
    return merged


class PrototypeGenerator(type):
    """Metaclass that collects :class:`Column` / :class:`TableProperty` / policies from class bodies."""

    def __new__(metacls, name, bases, namespace, **kwds):
        """Promote ``name: Type = Column(...)`` entries then build ``_prototype``."""
        namespace = dict(namespace)
        merged_annotations = _merge_class_annotations(bases, namespace)
        _promote_bare_columns_from_annotations(namespace, merged_annotations)

        cls = super(PrototypeGenerator, metacls).__new__(metacls, name, bases, namespace)
        columns, props, policies = [], [], []

        bases_namespace = {}
        for base in list(bases):
            bases_namespace.update(base.__dict__)
        bases_namespace.update(namespace)

        for attr_name, obj in bases_namespace.items():
            if isinstance(obj, Column):
                if type(obj) is Column:
                    raise BareColumnRequiresConcreteAnnotation(
                        "Bare Column(...) for {!r} must be written as name: dbsa.SomeType = Column(...).".format(attr_name)
                    )
                obj.name = attr_name
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
    """Class decorator that applies ``metaclass`` without ``__metaclass__`` syntax."""

    def wrapper(cls):
        """Rebuild ``cls`` with ``metaclass`` while preserving relevant ``__dict__`` keys."""
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
    """Declarative table schema; subclass and attach :class:`Column` / :class:`TableProperty` attributes."""

    table_prefix = ''
    _how_to_quote = '"{}"'
    _sample_value_function = 'MAX({c})'

    def __init__(self, schema, dialect=None, catalog=None, **values):
        """Instantiate with ``schema``; ``values`` set partition column ``value``s by name."""
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
        self.catalog = catalog
        self.dialect = None
        self.register_dialect(dialect)

    @property
    def partitions(self):
        """List of partition :class:`Partition` columns."""
        return [c for c in self._columns if c.partition]

    @property
    def properties(self):
        """Copy of table-level :class:`TableProperty` instances."""
        return self._props

    @property
    def table_name(self):
        """Snake_case name derived from the Python class name."""
        return re.sub('(?!^)([A-Z]+)', r'_\1', self.__class__.__name__).lower()

    @property
    def table_name_with_prefix(self):
        """``table_prefix`` + ``table_name``."""
        return self.table_prefix + self.table_name

    def get_properties(self):
        """Return the list of :class:`TableProperty` objects."""
        return self._props

    def get_property_by_type(self, type):
        """First property that is an instance of ``type`` (e.g. :class:`Sortkey`)."""
        for p in self.get_properties():
            if isinstance(p, type):
                return p

    def partition_definition(self, cleanup_fn=cleanup_fn):
        """``key=value`` path segments for all partitions that have values."""
        return '/'.join('{name}={value}'.format(
            name=p.name,
            value=cleanup_fn(p.value, quoted=False, dashed=True),
        ) for p in self.partitions if p.value is not None)

    def staging_table_name(self, cleanup_fn=cleanup_fn):
        """``stg_`` + optional partition suffix + physical table name."""
        named_partitions = '_'.join(cleanup_fn(c.value, quoted=False, dashed=False) for c in self.partitions if c.value)
        if not named_partitions: return 'stg_' + self.table_name
        return 'stg_' + '_'.join([named_partitions, self.table_name])

    def staging_table_name_with_prefix(self, cleanup_fn=cleanup_fn):
        """``table_prefix`` + :meth:`staging_table_name`."""
        return self.table_prefix + self.staging_table_name(cleanup_fn)

    def register_dialect(self, dialect):
        """Bind ``dialect`` templates to all columns and properties (no-op if ``dialect`` is None)."""
        if dialect is None: return

        for c in self._columns:
            c.register_dialect(dialect)

        for p in self._props:
            p._req_properties = dialect._req_properties.get(p.__class__)
            p._property_type = dialect._property_types.get(p.__class__)
            if not p._property_type: raise NotSupportedDialect

        self._how_to_quote = dialect._how_to_quote_table
        self._sample_value_function = dialect._sample_value_function
        self.dialect = dialect

    def _quote(self, text, quoted):
        """Quote ``text`` for SQL identifiers when ``quoted`` is true."""
        return text if not quoted else self._how_to_quote.format(text)

    def columns(self, include_partitions=True, filter_fn=None):
        """Column list, optionally excluding partitions or applying ``filter_fn``."""
        columns = self._columns if not filter_fn else filter(filter_fn, self._columns)
        return [c for c in columns if (not c.partition) or include_partitions]

    def column_names(self, include_partitions=True, filter_fn=None, as_list=False):
        """Set of column names, or list when ``as_list`` is true."""
        column_names = [c.name for c in self.columns(include_partitions=include_partitions, filter_fn=filter_fn)]
        if as_list: return column_names
        return set(column_names)

    def partition_names(self, as_list=False):
        """Partition column names as set or list."""
        partition_names = [p.name for p in self.partitions]
        if as_list: return partition_names
        return set(partition_names)

    def full_table_name(self, quoted=False, with_prefix=False, suffix=''):
        """``catalog.schema.table`` or ``schema.table`` with optional quoting and ``suffix``."""
        table_name = self._quote((self.table_name_with_prefix if with_prefix else self.table_name) + suffix, quoted)
        schema = self._quote(self.schema, quoted)
        if self.catalog is not None:
            catalog = self._quote(self.catalog, quoted)
            return '{}.{}.{}'.format(catalog, schema, table_name)
        else:
            return '{}.{}'.format(schema, table_name)

    def full_staging_table_name(self, cleanup_fn=cleanup_fn, quoted=False, with_prefix=False, suffix=''):
        """Like :meth:`full_table_name` but for the staging table name."""
        table_name = self._quote((self.staging_table_name_with_prefix(cleanup_fn=cleanup_fn) if with_prefix else self.staging_table_name(cleanup_fn=cleanup_fn)) + suffix, quoted)
        schema = self._quote(self.schema, quoted)
        if self.catalog is not None:
            catalog = self._quote(self.catalog, quoted)
            return '{}.{}.{}'.format(catalog, schema, table_name)
        else:
            return '{}.{}'.format(schema, table_name)

    def column_values(self, include_partitions=True, filter_fn=None):
        """Generator of :attr:`Column.default_load_value` strings for INSERT SELECT."""
        return (c.default_load_value for c in self.columns(include_partitions=include_partitions, filter_fn=filter_fn))

    def get_current_partition_params(self, params=None):
        """Map partition name to value for ``str.format`` on WHERE templates."""
        _params = {c.name: c.value for c in self.partitions if c.value is not None}
        _params.update(params or {})
        return _params

    def get_current_partition_condition(self, condition='', ignored_partitions=None, sep=' AND '):
        """``name = {name}`` predicates joined by ``sep``, plus optional extra ``condition``."""
        partition_names = {p.name for p in self.partitions} - set(ignored_partitions or [])
        partitions = [p for p in self.partitions if p.name in partition_names]
        conditions = ['{quoted_name} = {{{name}}}'.format(name=p.name, quoted_name=p.quoted_name) for p in partitions]
        if condition: conditions.append(condition)
        return sep.join(conditions)

    def set_catalog(self, catalog):
        """Change the optional catalog segment of generated names."""
        self.catalog = catalog

    def set_schema(self, schema):
        """Change the schema segment of generated names."""
        self.schema = schema

class Dialect(object):
    """Base SQL generator bound to a :class:`Table` instance."""

    _column_types = {}
    _req_properties = {}
    _property_types = {}
    _how_to_quote_table = '"{}"'
    _how_to_quote_column = '"{}"'
    _column_setter = '{} AS {}'
    _sample_value_function = 'MAX({c})'
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
        """Attach to ``table`` and register this dialect on all columns and properties."""
        self.table = table
        self.table.register_dialect(self)
        for fn in self._exposed_table_functions:
            if not hasattr(self, fn):
                setattr(self, fn, getattr(self.table, fn))

    def add_table_column(self, column):
        """Append a column at runtime (must already have ``name`` and be dialect-ready)."""
        self.table._columns.append(column)
        setattr(self.table, column.name, column)
        column.register_dialect(self)

    def to_markdown(self, header='###'):
        """Render the built-in markdown table documentation template."""
        import inspect
        return Template(MARKDOWN).render(t=self.table, inspect=inspect, header=header)

    def clone(self, **kwargs):
        """New dialect wrapping a fresh table instance with overridden partition kwargs."""
        return self.__class__(self.table.__class__(
            schema=self.table.schema,
            **kwargs,
        ))

    def lookup_policy(self, type_cls):
        """Return the policy instance registered under ``type_cls.__name__``."""
        return self.table._policies.get(type_cls.__name__)

    def resolve_policy(self, type_cls):
        """Run :meth:`TablePolicy.resolve` for ``type_cls`` if present."""
        lookup = self.lookup_policy(type_cls)
        if not lookup:
            return

        return lookup.resolve(self)

    def get_create_table(self, filter_fn=None, suffix=''):
        """``CREATE TABLE`` SQL; implemented per dialect."""
        raise NotImplemented()

    def get_drop_table(self, suffix=''):
        """``DROP TABLE`` SQL; implemented per dialect."""
        raise NotImplemented()

    def get_truncate_table(self, suffix=''):
        """``TRUNCATE TABLE`` SQL; implemented per dialect."""
        raise NotImplemented()

    def get_select(self, filter_fn=None, suffix='', condition='', transforms=None, limit=None):
        """``SELECT`` SQL over the table; implemented per dialect."""
        raise NotImplemented()

    def get_select_current_partition(self, filter_fn=None, condition='', params=None, ignored_partitions=None, transforms=None, suffix='', limit=None):
        """``SELECT`` restricted to the current partition predicate."""
        return self.get_select(
            filter_fn=filter_fn,
            suffix=suffix,
            transforms=transforms,
            limit=limit,
            condition=self.table.get_current_partition_condition(condition, ignored_partitions) \
                .format(**self.table.get_current_partition_params(params))
        )

    def get_delete_current_partition(self, condition='', params=None, ignored_partitions=None, suffix=''):
        """Delete rows matching the current partition (dialect-specific statement)."""
        return self.get_delete_from(
            condition=self.table.get_current_partition_condition(condition, ignored_partitions),
            params=self.table.get_current_partition_params(params),
            suffix=suffix,
        )

    def get_delete_from(self, condition=None, params=None, suffix=''):
        """``DELETE FROM`` SQL; implemented per dialect."""
        raise NotImplemented()

    def get_insert_into_from_table(self, source_table_name, filter_fn=None, suffix=''):
        """``INSERT INTO … SELECT`` from a named source table."""
        raise NotImplemented()

    def get_insert_into_via_select(self, select, filter_fn=None, embed_select=True, suffix=''):
        """``INSERT INTO … SELECT`` from an arbitrary SELECT string."""
        raise NotImplemented()

    def get_drop_current_partition_view(self, suffix='_latest'):
        """``DROP VIEW`` for the partition-scoped view name."""
        raise NotImplemented()

    def get_create_current_partition_view(self, suffix='_latest', condition='', ignored_partitions=None, params=None, transforms=None):
        """``CREATE OR REPLACE VIEW`` over :meth:`get_select_current_partition`."""
        raise NotImplemented()

    def get_sample_column_value(self, filter_fn=None, condition='', params=None, ignored_partitions=None, suffix='', limit=None):
        """``SELECT`` with aggregate per column (e.g. ``MAX``) for data-quality checks."""
        return self.get_select(
            filter_fn=filter_fn,
            suffix=suffix,
            limit=limit,
            transforms={
                column_name : self._sample_value_function
                for column_name in self.column_names(as_list=True)
            },
            condition=self.table.get_current_partition_condition(condition, ignored_partitions) \
                .format(**self.table.get_current_partition_params(params))
        )
