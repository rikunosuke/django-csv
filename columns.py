from typing import Any, List, Tuple

from django.db import models
from django.utils.functional import cached_property


class NoHeaderError(Exception):
    pass


class NoSuchFieldError(Exception):
    pass


class ColumnValidationError(Exception):
    pass


class WriteMixin:
    def validate_for_write(self) -> None:
        if not self.csv_output:
            return

        if self.w_index is None:
            raise ColumnValidationError(
                'Set `w_index` or `index` to specify the target column. '
                '`index` starts from 0. Set `csv_output` = False then '
                'CsvColumn does not write down any values.')

    def get_value_for_write(self, dictionary: dict, key: str, **kwargs):
        return dictionary.get(key, '')


class ReadMixin:
    def validate_for_read(self) -> None:
        if self.is_static or not self.model_field:
            return

        if self.r_index is None:
            raise ColumnValidationError(
                'Set `r_index` or `index` to specify the target column. '
                '`index` starts from 0. Set `model_field` = False then '
                'CsvColumn does not read any values.')

    def get_value_for_read(self, row: List[str], **kwargs):
        """
        CSV の値を読み込む際に呼び出される
        row: CSV の行をリスト化したもの
        """
        return row[self.r_index]


class BaseColumn(ReadMixin, WriteMixin):
    is_static = False
    is_relation = False

    def __init__(self, *, header: str = None, index: int = None,
                 w_index: int = None, r_index: int = None,
                 csv_output: bool = True, model_field: bool = True):
        """
        index: CSVの列の順番 0 から始まる
        w_index: 書き込みされる列の順番
        r_index: 読み込み時に参照する列の順番
        header: CSV の列名。
        csv_output: CSV に出力する場合は True
        model_field: model に渡す場合は True
        """
        self.header = header
        self.w_index = index if w_index is None else w_index
        self.r_index = index if r_index is None else r_index

        self.csv_output = csv_output
        self.model_field = model_field


class Column(BaseColumn):
    pass


class FieldColumn(BaseColumn):
    """
    CSV とモデルを動的に関連付ける
    """
    def get_value_for_write(self, instance, field_name: str, **kwargs) -> Any:
        """
        CSV に値を書き込む際に呼び出される
        instance: instance of Django Model
        field_name: field name of Django Model
        """
        return getattr(instance, field_name, '')


class StaticColumn(BaseColumn):
    """
    CSV とモデルを静的に関連づける
    CSV に依らない静的な値をモデルに渡したり、
    モデルに依らない値を CSVに書き込む際に使う
    出力ID などを想定
    """
    is_static = True

    def __init__(self, *, static_value: Any = '', **kwargs):
        """
        static_value: 静的な値 主に書き込み時に使われる
        """
        self.static_value = static_value
        super().__init__(**kwargs)

    def get_value_for_read(self, *args, **kwargs) -> Any:
        return self.static_value

    def get_value_for_write(self, *args, **kwargs) -> Any:
        return self.static_value


class ForeignColumn(BaseColumn):
    """
    ForeignKey 先のモデルとCSVデータを関連づける
    """
    is_relation = True

    def __init__(self, main, **kwargs):
        self._main = main
        super().__init__(**kwargs)

    def get_value_for_write(self, instance, field_name: str, **kwargs) -> Any:
        """
        CSV に値を書き込む際に呼び出される
        instance: models.Model のインスタンス
        field_name: models.Model のフィールド名
        """
        value = instance
        for field in field_name.split('__'):
            try:
                value = getattr(value, field)
            except AttributeError:
                raise NoSuchFieldError(
                    f'`{value.__class__.__name__}` does not have a'
                    f'field `{field}`')
        return value

    def _set_field_decorator(method):
        def set_field(self, *args, **kwargs):
            value = method(self, *args, **kwargs)
            self._main.set_field(
                kwargs.get('fieldname').split('__')[-1], value)
            return value
        return set_field

    set_field_decorator = staticmethod(_set_field_decorator)

    @_set_field_decorator
    def get_value_for_read(self, row: List[str], **kwargs):
        return super().get_value_for_read(row, **kwargs)


class ForeignColumns:
    def __init__(self, *, model: models.Model, fieldname: str,
                 callback: str = 'create_object'):

        self.model = model
        self.columns = []
        self.fieldname = fieldname
        self.field_kwargs = {}

        class NoCallbackError(Exception):
            pass

        if not hasattr(self, callback):
            raise NoCallbackError(f'{callback} method is not defined.')

        self.callback = callback

    def call_relations(self) -> Tuple[str, Any]:
        return getattr(self, self.callback)()

    def column(self, **kwargs):
        column = ForeignColumn(main=self, **kwargs)
        self.columns.append(column)
        return column

    def create_object(self):
        return self.model.objects.create(**self.field_kwargs)

    def get_object(self):
        return self.model.objects.get(**self.field_kwargs)

    def set_field(self, field, value):
        if field not in self.model_fields:
            raise NoSuchFieldError(
                f'`{self.model.__class__.__name__}` does not have a field '
                f'`{field}`')

        self.field_kwargs.update({field: value})

    @cached_property
    def model_fields(self):
        return [f.name for f in self.model._meta.get_fields()]