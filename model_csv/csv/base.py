import inspect
from typing import Generator, Any, List, Dict, TypeVar, Type, Union, Callable,\
    Optional

from django.db import models

from .. import writers
from ..columns import ForeignAttributeColumn, ForeignMethodColumn,\
    ForeignStaticColumn, BaseForeignColumn

READ_PREFIX = 'field_'
WRITE_PREFIX = 'column_'


class TableForRead:
    def __init__(self, table: List[list]) -> None:
        self.table = table
        self.validate()

    def validate(self):
        for col in self._meta.get_columns():
            col.validate_for_read()

        if not self._meta.get_columns(for_read=True):
            raise ValueError(
                f'{self.__class__.__name__} needs at least one column')

        indexes = self._meta.get_r_indexes()
        if len(indexes) != len(set(indexes)):
            raise ValueError(
                '`index` must be unique. Change `index` or `r_index`')

        if len(self.table[0]) < max(indexes):
            class ReadIndexOverColumnNumberError(Exception):
                pass

            raise ReadIndexOverColumnNumberError(
                f'column number: {len(self.table[1])} < '
                f'r_index: {max(indexes)}')

    def get_as_dict(self) -> Generator:
        """
        return generator of dict {'value_name': value, ...}
        """
        for row in self.table:
            values = self.read_from_row(row)
            yield self.field(values=values, static=self._static.copy())


class RowForRead:
    """
    A class to read values from each row.
    1. get value from csv. Call Column.get_value_for_read() method.
    2. change type of value from str to `column.to` type.
    3. apply `field_*` method change.
    4. apply `field` method change.
    """

    def field(self, values: dict, **kwargs):
        """
        The `values` are values fixed in `fields_*` methods.
        """
        return values

    def apply_method_change(self, values: dict) -> dict:
        """
        call method named `field_<attr_name>.`

        e.g.
        class BookCsv(ModelCsv):
            title = MethodColumn(...)

            def field_title(self, values: dict) -> Any:
                title = values['title']
                return title.replace('-', '')

        values: raw values got from csv.
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        updated = values.copy()
        for name, mthd in methods:
            if not name.startswith(READ_PREFIX):
                continue

            field_name = name.split(READ_PREFIX)[1]
            updated[field_name] = mthd(
                values=values.copy(), static=self._static.copy(),
            )
        return updated

    def read_from_row(self, row: List[str], is_relation: bool = False
                      ) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
        Relation は返さない
        """
        values = {
            col.value_name: self._meta.convert_from_str(
                col.get_value_for_read(row=row), to=col.to
            ) for col in self._meta.get_columns(
                for_read=True, is_relation=is_relation
            )
        }

        return self.apply_method_change(values)


class ModelRowForRead(RowForRead):
    def read_from_row(self, row: List[str], is_relation: bool = False
                      ) -> Dict[str, Any]:
        values = super().read_from_row(row, is_relation)
        values.update({
            prt.field_name: prt.get_instance(
                row=row, static=self._static.copy()
            ) for prt in self._meta.parts
        })
        return values

    def remove_extra_values(self, values: dict) -> dict:
        """
        remove values which is not in
        """
        fields = self._meta.model._meta.get_fields()

        return {
            k: v for k, v in values.items()
            if k in [f.name for f in fields] or k.endswith('_id')
        }


class CsvForRead(RowForRead, TableForRead):
    pass


class ModelCsvForRead(ModelRowForRead, TableForRead):
    def get_instances(self) -> Generator:
        """
        CSV から作成した Model のインスタンスを返すジェネレーター
        """
        for values in self.get_as_dict():
            values = self.remove_extra_values(values)
            yield self._meta.model(**values)

    def bulk_create(self, batch_size=100) -> None:
        instances = list(self.get_instances())
        offset = 0

        while True:
            created = instances[offset:offset + batch_size]
            if not created:
                break

            self._meta.model.objects.bulk_create(created)
            offset += batch_size


class RowForWrite:
    def get_headers(self) -> List[str]:
        return self.render_row({
            column.get_w_index(): column.header
            for column in self._meta.get_columns(for_write=True)
        })

    def get_row_value(self, instance, is_relation: bool = False) -> Dict[int, str]:
        """
        return {w_index: value}
        """
        def __get_row_from_column(_column):
            method_name = WRITE_PREFIX + _column.method_suffix
            if not _column.is_static and hasattr(self, method_name):
                _value = getattr(self, method_name)(
                    instance=instance, static=self._static.copy()
                )
            else:
                _value = _column.get_value_for_write(instance=instance)

            return self._meta.convert_to_str(_value, to=_column.to)

        row = {
            column.get_w_index(): __get_row_from_column(column)
            for column in self._meta.get_columns(
                for_write=True, is_relation=is_relation
            )
        }

        for part in self._meta.parts:
            row.update(part.get_row_value(instance, is_relation=True))

        return row

    def render_row(self, maps: Dict[int, Any]) -> List[str]:
        """
        convert maps from dict to list ordered by index.
        maps: {index: value}
        """
        def __get_value_from_map(_i) -> Optional[str]:
            try:
                return maps[_i]
            except KeyError:
                if self._meta.insert_blank_column:
                    return ''

        return list(filter(
            lambda x: x is not None,
            [
                __get_value_from_map(i)
                for i in range(max(list(maps.keys())) + 1)
             ]
        ))


class CsvForWrite(RowForWrite):
    def __init__(self, instances):
        self.instances = instances
        self.validate()

    def get_response(self, writer: writers.Writer, header: bool = True):
        writer.write_down(table=self.get_table(header=header))
        return writer.make_response()

    def get_table(self, header: bool = True) -> list[list]:
        """
        2D list created from instances.
        """
        table = [self.get_headers()] if header else []
        table += [
            self.render_row(self.get_row_value(instance=instance))
            for instance in self.instances
        ]

        return table

    def validate(self):
        for col in  self._meta.get_columns():
            col.validate_for_write()

        if not self._meta.get_columns(for_write=True):
            raise ValueError(
                f'{self.__class__.__name__} needs at least one column'
            )

        index = self._meta.get_w_indexes()
        if len(index) != len(set(index)):
            raise ValueError(
                '`index` must be unique. Change `index` or `w_index`')


BaseCsvType = TypeVar('BaseCsvType', bound='BaseCsv')


class BaseCsv:
    """
    Manage Class to create CsvForRead and CsvForWrite.
    """
    read_class = None
    write_class = CsvForWrite

    class ReadModeIsProhibited(Exception):
        pass

    class WriteModeIsProhibited(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self._static = {}
        super().__init__(*args, **kwargs)

    def set_static_column(self, column_name: str, value: Any) -> None:
        """
        StaticColumn の static_value を書き換える
        """
        column = self._meta.get_column(name=column_name)
        if not column or not column.is_static:
            raise ValueError(f'`{column_name}` is not a static column.')

        column.static_value = value

    def set_static(self, key, value) -> None:
        self._static.update({key: value})

    @classmethod
    def for_read(cls, table: List[list]) -> BaseCsvType:
        """
        CSV 読み込みようのインスタンスを返す
        """
        if not cls._meta.read_mode:
            raise cls.ReadModeIsProhibited('Read Mode is prohibited')

        return type(
            f'{cls.__name__}ForRead', (cls, cls.read_class),
            {'_meta': cls._meta}
        )(table=table)

    @classmethod
    def for_write(cls, instances) -> BaseCsvType:
        """
        CSV 書き込みようのインスタンスを返す
        """
        if not cls._meta.write_mode:
            raise cls.WriteModeIsProhibited('Write Mode is prohibited')

        return type(
            f'{cls.__name__}ForWrite', (cls, cls.write_class),
            {'_meta': cls._meta}
        )(instances=instances)


class PartForRead(ModelRowForRead):
    def get_instance(self, row: List[str], static: dict) -> models.Model:
        self._static = static.copy()  # inject static from main csv.

        values = self.field(
            values=self.read_from_row(row, is_relation=True)
        )
        values = self.remove_extra_values(values)
        return self._callback(values=values)

    def get_or_create_object(self, values: dict) -> models.Model:
        return self.model.objects.get_or_create(**values)[0]

    def create_object(self, values: dict) -> models.Model:
        return self.model.objects.create(**values)

    def get_object(self, values: dict) -> models.Model:
        return self.model.objects.get(values)


class PartForWrite(RowForWrite):
    def get_row_value(self, instance, is_relation: bool = False
                      ) -> Dict[int, str]:
        # get foreign model.
        relation_instance = getattr(instance, self.field_name)
        if not isinstance(relation_instance, self.model):
            raise ValueError(f'Wrong field name. `{self.field_name}` is not '
                             f'{self.model.__class__.__name__}.')
        return super().get_row_value(relation_instance, is_relation=is_relation)


class BasePart(PartForWrite, PartForRead):
    def __init__(self, field_name: str,
                 callback: Union[str, Callable] = 'get_or_create_object',
                 **kwargs):

        if self._meta.model is None:
            mcsv_class_name = self.__class__.__name__.split('Part', 1)[0]
            raise ValueError(
                f'django model is not defined in meta class of {mcsv_class_name}'
            )

        self.model = self._meta.model
        self.field_name = field_name
        self._static = {}

        if isinstance(callback, str):
            self._callback = getattr(self, callback)
        elif callable(callback):
            self._callback = callback
        else:
            raise ValueError('`callback` must be str or callable.')

        # Don't call super().__init__ cos column validations should not run.

    def _add_column(self, column_class: Type[BaseForeignColumn], **kwargs
                    ) -> BaseForeignColumn:
        column = column_class(field_name=self.field_name,  **kwargs)
        self._meta.columns.append(column)
        return column

    # Use UpperCamel case.
    # e.g. prt.AttributeColumn()
    def AttributeColumn(self, attr_name: str, **kwargs):
        # attr_name is required
        return self._add_column(
            ForeignAttributeColumn, attr_name=attr_name, **kwargs)

    def MethodColumn(self, **kwargs):
        return self._add_column(ForeignMethodColumn, **kwargs)

    def StaticColumn(self, **kwargs):
        return self._add_column(ForeignStaticColumn, **kwargs)
