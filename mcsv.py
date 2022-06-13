import inspect
from datetime import datetime, date
from typing import Generator, Any, List, Dict, Optional, Iterable, TypeVar

import copy

from . import writers
from .columns import BaseColumn, AttributeColumn


class CsvForRead:
    """
    読み込み用のクラス
    .for_read() で作成する
    Model のフィールドに渡す値を動的に作成する場合は
    field_*フィールド名* のメソッドを定義する。
    """

    class RIndexOverColumnNumberError(Exception):
        pass

    def __init__(self, table: List[list]) -> None:
        self.table = table
        super().__init__(table)

    def _run_column_validation(self):
        for col in self._meta.get_columns():
            col.validate_for_read()

        indexes = self._meta.get_r_indexes()
        if len(indexes) != len(set(indexes)):
            raise ValueError(
                '`index` must be unique. Change `index` or `r_index`')

        if len(self.table[1]) < max(indexes):
            raise CsvForRead.RIndexOverColumnNumberError(
                f'column number: {len(self.table[1])} < '
                f'r_index: {max(indexes)}')

    def field(self, values: dict, **kwargs):
        """
        The `values` are values fixed in `fields_*` methods.
        """
        return values

    def _apply_method_change(self, values: dict) -> dict:
        """
        動的なフィールドの値を作成する
        values: 各 Column から取り出した {field 名: 値} の辞書型
        def field_*(self, values: dict) -> Any:
        * 部分をフィールド名にする。
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        prefix = 'field_'
        updated = values.copy()
        for name, mthd in methods:
            if not name.startswith(prefix):
                continue

            field_name = name.split(prefix)[1]
            updated[field_name] = mthd(
                values=values.copy(), static=self._static.copy(),
            )
        return self.field(values=updated, static=self._static.copy())

    def get_values(self, row: List[str]) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
        Column インスタンスが read_value=False の場合は含めない
        """
        values = {}
        for column in self._meta.get_columns(for_read=True):
            values.update({
                column.name: column.get_value_for_read(
                    row=row, fieldname=column.name)
            })

        return self._convert_from_str(self._apply_method_change(values))

    def _convert_from_str(self, values: dict) -> dict:
        updated = values.copy()
        for k, v in values.items():
            if v in self._meta.as_true:
                updated[k] = True
                continue

            elif v in self._meta.as_false:
                updated[k] = False
                continue

            try:
                updated[k] = datetime.strptime(v, self._meta.datetime_format)
                continue
            except ValueError:
                pass

        return updated


class ModelCsvForRead(CsvForRead):
    def get_values(self, row: List[str]) -> Dict[str, Any]:
        values = super().get_values(row)
        # values = self._make_relations(values)
        values = self._remove_extra_values(values)
        return values

    def get_instances(self) -> Generator:
        """
        CSV から作成した Model のインスタンスを返すジェネレーター
        """
        for row in self.table:
            yield self._meta.model(**self.get_values(row))

    def bulk_create(self, batch_size=100) -> None:
        """
        CSV から作成した Model を DB に保存する
        batch_size: 一度にジェネレーターから作成するインスタンスの最大値
        """
        models = []
        instances = list(self.get_instances())
        length = len(instances)
        for i, instance in enumerate(instances, 1):
            models.append(instance)
            if len(models) % batch_size == 0 or i == length:
                self._meta.model.objects.bulk_create(models)
                models = []

    def _remove_extra_values(self, values: dict) -> dict:
        """
        Model にないフィールドの値を取り除く
        """
        cleaned_values = {}
        for key, value in values.items():
            if key in self.field_names:
                cleaned_values.update({key: value})
        return cleaned_values

    def _make_relations(self, values: dict) -> dict:
        for cols in self._relations:
            values.update({cols.fieldname: cols.call_relations()})
        return values

    @property
    def field_names(self) -> list:
        """
        Model の存在するフィールド名のリスト
        """
        return [field.name for field in self._meta.model._meta.get_fields()]


class CsvForWrite:
    """
    書き込み用のクラス
    .for_write() で作成する
    列の値に編集を加える場合は
    column_*列名* のメソッドを定義する
    padding: w_index の値が連続していない場合、True の場合は空欄の列を詰める
    """
    padding = False

    def __init__(self, queryset):
        self.queryset = queryset
        super().__init__(queryset)

    def get_response(self, writer: writers.Writer, header: bool = True):
        return writer.make_response(table=self.get_table(header=header))

    def get_table(self, header: bool = True):
        """
        ヘッダー情報と Model インスタンスから作成した値を2次元のリストで返す
        """
        table = []
        if header:
            table.append(self._get_header())

        for instance in self.queryset:
            # self._render_row に渡すために
            # {index: value} の辞書型をつくる
            row = self._get_row_value(instance)
            table.append(self._render_row(row))

        return table

    def _get_header(self) -> List[str]:
        """
        ヘッダーの情報を {列の順番: 列名} の辞書型で返す
        Column に header が定義されていない場合は Column
        のクラス変数名を列名にする
        """
        headers = {}
        for column in self._meta.get_columns(for_write=True):
            headers.update({
                column.w_index: getattr(column, 'header', column.name)
            })

        return self._render_row(headers)

    def _get_row_value(self, instance) -> Dict[int, str]:
        """
        行の値を {列名: 値} で返す
        """
        row = {}
        for column in self._meta.get_columns(for_write=True):
            if hasattr(self, f'column_{column.name}'):
                value = getattr(self, f'column_{column.name}')(
                    instance=instance, static=self._static.copy())
            else:
                value = column.get_value_for_write(instance)

            row.update({
                column.w_index: value
            })
        return row

    def _render_row(self, maps: Dict[int, Any]) -> List[str]:
        """
        列の順番 通りに並び替えた 値 のリストを返す
        maps: {列の順番: 値} の辞書型
        """
        row = []
        for i in range(max(list(maps.keys())) + 1):
            try:
                value = maps[i]
            except KeyError:
                if self.padding:
                    continue
                value = ''

            row.append(self._convert_to_str(value))

        return row

    def _convert_to_str(self, value):
        if type(value) == datetime:
            return value.strftime(self._meta.datetime_format)

        elif type(value) == bool:
            return self._meta.show_true if value else self._meta.show_false

        else:
            return str(value)

    def _run_column_validation(self):
        for col in self._meta.get_columns():
            col.validate_for_write()

        index = self._meta.get_w_indexes()
        if len(index) != len(set(index)):
            raise ValueError(
                '`index` must be unique. Change `index` or `w_index`')


BaseCsvType = TypeVar('BaseCsvType', bound='BaseCsv')


class BaseCsv:
    _static = {}

    read_class = None
    write_class = CsvForWrite

    class NotAllowedReadType(Exception):
        pass

    class NotAllowedWriteType(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self._run_column_validation()

    def set_static_column(self, column_name: str, value: Any) -> None:
        """
        StaticColumn の static_value を書き換える
        """
        column = getattr(self, column_name, None)
        if getattr(column, 'is_static', False):
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
            raise cls.NotAllowedReadType('Not Allowed read mode.')

        return type(f'{cls.__name__}ForRead', (cls.read_class, cls), {}
                    )(table=table)

    @classmethod
    def for_write(cls, queryset) -> BaseCsvType:
        """
        CSV 書き込みようのインスタンスを返す
        """
        if not cls._meta.write_mode:
            raise cls.NotAllowedWriteType('Not Allowed write mode')

        return type(f'{cls.__name__}ForWrite', (cls.write_class, cls), {}
                    )(queryset=queryset)


class Options:

    DEFAULT_ATTRS = {
        'read_mode': True,
        'write_mode': True,
        'datetime_format': '%Y-%m-%d %H:%M:%S',
        'show_true': 'yes',
        'show_false': 'no',
        'as_true': ['yes', 'Yes'],
        'as_false': ['no', 'No']
    }

    ALLOWED_META_ATTR_NAMES = (
        'read_mode',
        'write_mode',
        'datetime_format',
        'show_true',
        'show_false',
        'as_true',
        'as_false',
        'auto_assign',
    )

    APPLY_ONLY_MAIN_META_ATTR_NAMES = tuple()

    class UnknownAttribute(Exception):
        pass

    class UnknownColumn(Exception):
        pass

    def __init__(self, meta: type, columns: Dict[str, BaseColumn],
                 *args, **kwargs):
        meta = copy.deepcopy(meta)

        # set default value to meta class
        for key, value in self.DEFAULT_ATTRS.items():
            if not hasattr(meta, key):
                setattr(meta, key, value)

        # validate meta attrs and set attr to Options.
        for attr_name in dir(meta):
            if attr_name.startswith('_'):
                continue

            if attr_name not in self.ALLOWED_META_ATTR_NAMES:
                raise self.UnknownAttribute(
                    f'Unknown Attribute is defined. `{attr_name}`')

            setattr(self, attr_name, getattr(meta, attr_name))

        self.columns = []
        for name, column in columns.copy().items():
            column.name = column.attr_name or name
            self.columns.append(column)

        if getattr(self, 'auto_assign', False):
            self.assign_number()

    @staticmethod
    def filter_columns(*, columns: List[BaseColumn], r_index: bool = None,
                       w_index: bool = None, for_write: bool = None,
                       for_read: bool = None, read_value: bool = None,
                       write_value: bool = None, is_static: bool = None,
                       original: bool = False) -> list:
        """
        filter passed columns and return them if they match conditions.
        """
        other_params = not all(map(
            lambda x: x is None,
            [r_index, is_static, read_value, write_value]))

        if for_read is not None:
            if other_params or original:
                raise TypeError('`for_write` with other params not supported')

            if for_read:
                columns = [
                    col for col in columns
                    if col.is_static or col.r_index is not None
                ]
                read_value = True
            else:
                columns = [
                    col for col in columns
                    if not col.read_value or
                    (not col.is_static and col.r_index is None)
                ]

        if for_write is not None:
            if other_params or original:
                raise TypeError('`for_write` with other params not supported')

            if for_write:
                w_index = True
                write_value = True
            else:
                columns = [
                    col for col in columns
                    if col.w_index is None or not col.write_value
                ]

        if r_index is not None:
            columns = [col for col in columns if any([
                isinstance(col.get_r_index(original), int) and r_index,
                col.get_r_index(original) is None and not r_index,
            ])]

        if w_index is not None:
            columns = [col for col in columns if any([
                isinstance(col.get_w_index(original), int) and w_index,
                col.get_w_index(original) is None and not w_index,
            ])]

        if read_value is not None:
            columns = [col for col in columns if col.read_value == read_value]

        if write_value is not None:
            columns = [col for col in columns if col.write_value == write_value]

        if is_static is not None:
            columns = [col for col in columns if col.is_static == is_static]

        return columns

    @staticmethod
    def get_unassigned(assigned: list) -> Iterable:
        """
        return the list of index which is not assigned yet.
        """
        i = 0
        while True:
            if i not in assigned:
                yield i

            i += 1

    def get_columns(self, *, r_index: bool = None, w_index: bool = None,
                    for_write: bool = None, for_read: bool = None,
                    read_value: bool = None, write_value: bool = None,
                    is_static: bool = None, original: bool = False,):

        return self.filter_columns(
            r_index=r_index, w_index=w_index, for_read=for_read,
            for_write=for_write, read_value=read_value, write_value=write_value,
            is_static=is_static, original=original, columns=self.columns)

    def get_column(self, name: str) -> BaseColumn:
        for col in self.columns:
            if col.name == name:
                return col
        else:
            raise self.UnknownColumn(f'UnknownColumn `{name}`')

    def assign_number(self) -> None:
        """
        Set r and w indexes to the columns not having original indexes.
        If run, all indexes automatically assigned are overwritten.
        """
        for i, col in zip(self.get_unassigned(self.get_r_indexes(True)),
                          self.get_columns(
                              r_index=False, original=True, read_value=True)):
            col.r_index = i

        for i, col in zip(self.get_unassigned(self.get_w_indexes(True)),
                          self.get_columns(
                              w_index=False, original=True, write_value=True)):
            col.w_index = i

    def get_r_indexes(self, original: bool = False) -> list:

        return [col.get_r_index(original) for col in self.get_columns(
            r_index=True, original=original)]

    def get_w_indexes(self, original: bool = False) -> list:

        return [col.get_w_index(original) for col in self.get_columns(
            w_index=True, original=original)]


class ModelOptions(Options):
    APPLY_ONLY_MAIN_META_ATTR_NAMES = Options.APPLY_ONLY_MAIN_META_ATTR_NAMES + (
        'model',
        'fields',
    )

    ALLOWED_META_ATTR_NAMES = Options.ALLOWED_META_ATTR_NAMES + (
        'model',
        'fields'
    )

    def __init__(self, meta: Optional[type], columns: Dict[str, BaseColumn]):
        model = getattr(meta, 'model', None)
        if not model:
            super().__init__(meta, columns)
            return

        field_names = getattr(meta, 'fields')

        if field_names == '__all__':
            fields = [f for f in model._meta.get_fields()
                      if not f.auto_created and not f.is_relation]
        else:
            fields = [model._meta.get_field(name) for name in field_names]

        unassigned_r = self.get_unassigned([
            col.get_r_index(original=True) for col in self.filter_columns(
                r_index=True, original=True, columns=list(columns.values()))
        ])
        unassigned_w = self.get_unassigned([
            col.get_w_index(original=True) for col in self.filter_columns(
                w_index=True, original=True, columns=list(columns.values()))
        ])

        column_names = columns.keys()
        for r, w, f in zip(unassigned_r, unassigned_w, fields):
            if f.name in column_names:
                continue

            header = getattr(f, 'verbose_name', f.name)
            columns.update({
                f.name: AttributeColumn(r_index=r, w_index=w, header=header)
            })

        super().__init__(meta, columns)


class CsvMetaClass(type):
    """
    create Options class from Meta and add _meta to attrs.
    """
    option_class = Options

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        if not any([isinstance(b, mcs) for b in bases]):
            return super().__new__(mcs, name, bases, attrs)

        # meta クラスを対応
        attrs['_meta'] = mcs.option_class(
            meta=mcs.__get_meta(attrs.get('Meta'), bases),
            columns=dict(
                mcs.__concat_columns(bases=bases, attrs=attrs).items()
            )
        )

        return super().__new__(mcs, name, bases, attrs)

    @classmethod
    def __concat_columns(mcs, bases: tuple, attrs: dict) -> dict:
        col_dict = {}
        for attr_name, attr in attrs.items():
            if isinstance(attr, BaseColumn):
                setattr(attr, 'name', attr_name)
                col_dict.update({attr_name: attr})

        for base in bases:
            if hasattr(base, '_meta'):
                col_dict.update({
                    col.name: col for col in base._meta.get_columns()})

        return col_dict

    @classmethod
    def __get_meta(mcs, meta: Optional[type], bases: tuple) -> type:
        metas = list(
            copy.deepcopy(getattr(base, 'Meta'))
            for base in bases if hasattr(base, 'Meta')
        )

        for meta in metas:
            for attr in mcs.option_class.APPLY_ONLY_MAIN_META_ATTR_NAMES:
                if hasattr(meta, attr):
                    delattr(meta, attr)
        if meta:
            metas.append(meta)

        return type('Meta', tuple(set(metas)), {})


class ModelCsvMetaClass(CsvMetaClass):
    option_class = ModelOptions


class Csv(BaseCsv, metaclass=CsvMetaClass):
    read_class = CsvForRead


class ModelCsv(BaseCsv, metaclass=ModelCsvMetaClass):
    read_class = ModelCsvForRead
