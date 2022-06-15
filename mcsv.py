import inspect
from datetime import datetime
from typing import Generator, Any, List, Dict, TypeVar, Type

from django.db import models

from . import writers
from .columns import ForeignColumn
from .metaclasses import CsvMetaclass, ModelCsvMetaclass, PartMetaclass

READ_PREFIX = 'field_'
WRITE_PREFIX = 'column_'


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
        updated = values.copy()
        for name, mthd in methods:
            if not name.startswith(READ_PREFIX):
                continue

            field_name = name.split(READ_PREFIX)[1]
            updated[field_name] = mthd(
                values=values.copy(), static=self._static.copy(),
            )
        return self.field(values=updated, static=self._static.copy())

    def get_values(self, row: List[str]) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
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
            updated[k] = self._meta.convert_from_str(v)

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
            method_name = WRITE_PREFIX + column.name
            if hasattr(self, method_name):
                value = getattr(self, method_name)(
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


class Csv(BaseCsv, metaclass=CsvMetaclass):
    read_class = CsvForRead


class ModelCsv(BaseCsv, metaclass=ModelCsvMetaclass):
    read_class = ModelCsvForRead


class Part(metaclass=PartMetaclass):
    def __init__(self, *, model: Type[models.Model] = None, field_name: str,
                 callback: str = 'create_object', **kwargs):

        self.model = model or self._meta.model
        self.columns = []
        self.field_name = field_name
        self.field_kwargs = {}
        self.__callback = getattr(self, callback)

    def ForeignColumn(self, **kwargs):
        column = ForeignColumn(main=self, field_name=self.field_name, **kwargs)
        self.columns.append(column)
        return column

    def field(self, values: dict, static: dict, **kwargs) -> dict:
        return values

    def call_relations(self, static: dict):
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        updated = self.field_kwargs.copy()
        for name, mthd in methods:
            if not name.startswith(READ_PREFIX):
                continue

            field_name = name.split(READ_PREFIX)[1]
            updated[field_name] = mthd(
                values=self.field_kwargs.copy(), static=static.copy(),
            )
        updated = self.field(values=updated, static=static.copy())

        return self.__callback(values={
            k: self._meta.convert_from_str(v) for k, v in updated
        })

    # TODO: 書き込み版を作成する 多分むずい

    def create_object(self, values: dict):
        return self.model.objects.create(**values)

    def get_object(self, values: dict):
        return self.model.objects.get(values)

    def set_field(self, field, value):
        self.field_kwargs.update({field: value})
