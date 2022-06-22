import inspect
from datetime import datetime
from typing import Generator, Any, List, Dict, TypeVar, Type, Union, Callable

from django.db import models

from django_csv import writers
from django_csv.columns import ForeignAttributeColumn, ForeignMethodColumn, \
    ForeignStaticColumn

READ_PREFIX = 'field_'
WRITE_PREFIX = 'column_'


class TableForRead:
    def __init__(self, table: List[list]) -> None:
        self.table = table
        super().__init__()

    def run_column_validation(self):
        for col in self._meta.get_columns():
            col.validate_for_read()

        indexes = self._meta.get_r_indexes()
        if len(indexes) != len(set(indexes)):
            raise ValueError(
                '`index` must be unique. Change `index` or `r_index`')

        if len(self.table[1]) < max(indexes):
            class ReadIndexOverColumnNumberError(Exception):
                pass

            raise ReadIndexOverColumnNumberError(
                f'column number: {len(self.table[1])} < '
                f'r_index: {max(indexes)}')

    def get_as_dict(self) -> Generator:
        """
        CSV から作成した Model
        """
        for row in self.table:
            values = self.read_from_row(row)
            yield self.field(values, staitc=self._static.copy())


class RowForRead:
    """
    読み込み用のクラス
    .for_read() で作成する
    Model のフィールドに渡す値を動的に作成する場合は
    field_*フィールド名* のメソッドを定義する。
    """

    def field(self, values: dict, **kwargs):
        """
        The `values` are values fixed in `fields_*` methods.
        """
        return values

    def apply_method_change(self, values: dict) -> dict:
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
        return updated

    def read_from_row(self, row: List[str], is_relation: bool = False
                      ) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
        Relation は返さない
        """
        values = {}
        for column in self._meta.get_columns(
                for_read=True, is_relation=is_relation):
            values.update({
                column.name: column.get_value_for_read(row=row)
            })
        values = self.convert_from_str(values)

        return self.apply_method_change(values)

    def convert_from_str(self, values: dict) -> dict:
        updated = values.copy()
        for k, v in values.items():
            updated[k] = self._meta.convert_from_str(v)

        return updated


class ModelRowForRead(RowForRead):
    def read_from_row(self, row: List[str], is_relation: bool = False
                      ) -> Dict[str, Any]:
        values = super().read_from_row(row, is_relation)
        for part in self._meta.parts:
            values[part.field_name] = part.get_instance(
                row=row, static=self._static.copy())

        values = self.remove_extra_values(values)
        return values

    def get_instances(self) -> Generator:
        """
        CSV から作成した Model のインスタンスを返すジェネレーター
        """
        for row in self.table:
            values = self.field(
                values=self.read_from_row(row), static=self._static.copy())
            yield self._meta.model(**values)

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

    def remove_extra_values(self, values: dict) -> dict:
        """
        Model にないフィールドの値を取り除く
        """
        cleaned_values = {}
        for key, value in values.items():
            if key in self.field_names:
                cleaned_values.update({key: value})
        return cleaned_values

    @property
    def field_names(self) -> list:
        """
        Model の存在するフィールド名のリスト
        """
        return [field.name for field in self._meta.model._meta.get_fields()]


class TableForWrite:
    def __init__(self, queryset):
        self.queryset = queryset
        self.padding = getattr(self._meta, 'padding', False)
        super().__init__()

    def get_response(self, writer: writers.Writer, header: bool = True):
        return writer.make_response(table=self.get_table(header=header))

    def get_table(self, header: bool = True):
        """
        ヘッダー情報と Model インスタンスから作成した値を2次元のリストで返す
        """
        table = []
        if header:
            table.append(self.get_header())

        for instance in self.queryset:
            # self._render_row に渡すために
            # {index: value} の辞書型をつくる
            row = self.get_row_value(instance)
            table.append(self.convert_to_str(self._render_row(row)))

        return table

    def run_column_validation(self):
        for col in self._meta.get_columns():
            col.validate_for_write()

        index = self._meta.get_w_indexes()
        if len(index) != len(set(index)):
            raise ValueError(
                '`index` must be unique. Change `index` or `w_index`')


class RowForWrite:
    """
    書き込み用のクラス
    .for_write() で作成する
    列の値に編集を加える場合は
    column_*列名* のメソッドを定義する
    """
    def get_header(self) -> List[str]:
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

    def get_row_value(self, instance, is_relation: bool = False) -> Dict[int, str]:
        """
        return {w_index: value}
        """
        row = {}
        for column in self._meta.get_columns(
                for_write=True, is_relation=is_relation):
            method_name = WRITE_PREFIX + column.name
            if hasattr(self, method_name):
                value = getattr(self, method_name)(
                    instance=instance, static=self._static.copy())
            else:
                value = column.get_value_for_write(instance)

            row.update({
                column.w_index: value
            })

        for part in self._meta.parts:
            row.update(part.get_row_value(instance, is_relation=True))

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

            row.append(value)

        return row

    def convert_to_str(self, row: list) -> list:
        updated = []
        for value in row:
            if type(value) == datetime:
                updated.append(value.strftime(self._meta.datetime_format))

            elif type(value) == bool:
                updated.append(
                    self._meta.show_true if value else self._meta.show_false)

            else:
                updated.append(str(value))

        return updated


class CsvForWrite(RowForWrite, TableForWrite):
    pass


BaseCsvType = TypeVar('BaseCsvType', bound='BaseCsv')


class BaseCsv:
    read_class = None
    write_class = CsvForWrite

    class ReadModeIsProhibited(Exception):
        pass

    class WriteModeIsProhibited(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self.run_column_validation()
        self._static = {}

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
            raise cls.ReadModeIsProhibited('Read Mode is prohibited')

        return type(f'{cls.__name__}ForRead', (cls.read_class, cls), {}
                    )(table=table)

    @classmethod
    def for_write(cls, queryset) -> BaseCsvType:
        """
        CSV 書き込みようのインスタンスを返す
        """
        if not cls._meta.write_mode:
            raise cls.WriteModeIsProhibited('Write Mode is prohibited')

        return type(f'{cls.__name__}ForWrite', (cls.write_class, cls), {}
                    )(queryset=queryset)


class CsvForRead(RowForRead, TableForRead):
    pass


class ModelCsvForRead(ModelRowForRead, TableForRead):
    pass


class PartForRead(ModelRowForRead):
    def get_instance(self, row: List[str], static: dict) -> models.Model:
        self._static = static.copy()  # inject static from main csv.

        values = super().read_from_row(row, is_relation=True)
        return self._callback(values=values)

    def get_or_create_object(self, values: dict) -> models.Model:
        return self.model.objects.get_or_create(**values)[0]

    def create_object(self, values: dict) -> models.Model:
        return self.model.objects.create(**values)

    def get_object(self, values: dict) -> models.Model:
        return self.model.objects.get(values)

    @property
    def field_names(self) -> list:
        """
        Model の存在するフィールド名のリスト
        """
        return [field.name for field in self.model._meta.get_fields()]


class PartForWrite(RowForWrite):
    def get_row_value(self, instance, is_relation: bool = False
                      ) -> Dict[int, str]:
        instance = getattr(instance, self.field_name)
        if not isinstance(instance, self.model):
            raise ValueError(f'Wrong field name. `{self.field_name}` is not '
                             f'{self.model.__class__.__name__}.')
        return super().get_row_value(instance, is_relation=is_relation)


class BasePart(PartForWrite, PartForRead):
    def __init__(self, field_name: str, model: Type[models.Model] = None,
                 callback: Union[str, Callable] = 'get_or_create_object',
                 **kwargs):
        self.model = model or self._meta.model
        self.field_name = field_name
        self.padding = getattr(self._meta, 'padding', False)  # for write.
        self._static = {}

        if isinstance(callback, str):
            self._callback = getattr(self, callback)
        elif callable(callback):
            self._callback = callback
        else:
            raise ValueError('`callback` must be str or callable.')

        # Don't call super().__init__ not to run validation.

    def AttributeColumn(self, **kwargs) -> ForeignAttributeColumn:
        column = ForeignAttributeColumn(field_name=self.field_name, **kwargs)
        self._meta.columns.append(column)
        return column

    def MethodColumn(self, **kwargs) -> ForeignMethodColumn:
        column = ForeignMethodColumn(field_name=self.field_name, **kwargs)
        self._meta.columns.append(column)
        return column

    def StaticColumn(self, **kwargs) -> ForeignStaticColumn:
        column = ForeignStaticColumn(field_name=self.field_name, **kwargs)
        self._meta.columns.append(column)
        return column
