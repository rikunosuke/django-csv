from typing import Any, List, Tuple, Optional

from django.db import models
from django.utils.functional import cached_property


class NoHeaderError(Exception):
    pass


class NoSuchFieldError(Exception):
    pass


class ColumnValidationError(Exception):
    pass


class WriteColumnMixin:

    def validate_for_write(self) -> None:
        if not self.write_value:
            return

        # Even if this column is StaticColumn, index for write is required.
        if self.w_index is None:
            raise ColumnValidationError(
                'Set `w_index` or `index` to specify the target column. '
                '`index` starts from 0. Set `write_value` = False and '
                'CsvColumn does not write down any values.')

    def get_value_for_write(self, dictionary: dict, key: str, **kwargs):
        return ''


class ReadColumnMixin:
    def validate_for_read(self) -> None:
        if self.is_static or not self.read_value:
            return

        if self.r_index is None:
            raise ColumnValidationError(
                'Set `r_index` or `index` to specify the target column. '
                '`index` starts from 0. Set `read_value` = False then '
                'CsvColumn does not read any values.')

    def get_value_for_read(self, row: List[str], **kwargs):
        """
        return the value for read mode.
        """
        return row[self.r_index]


class BaseColumn(ReadColumnMixin, WriteColumnMixin):
    is_static = False
    is_relation = False

    def __init__(self, *, header: str = None, index: int = None,
                 w_index: int = None, r_index: int = None,
                 write_value: bool = True, read_value: bool = True,
                 attr_name: Optional[str] = None):
        """
        index: csv column index. Start from 0.
        w_index: csv column index this column use when write down.
        r_index: csv column index this column use when read.
        header: csv column header. Use write section.
        write_value: if False then this column does not write down any value.
        read_value: if False then this column deos not pass any value.
        attr_name: The name Column use when retrieve value from instance.
        """
        self.header = header

        if read_value:
            self.__r_index = self.__original_r_index = index if (
                    r_index is None) else r_index
        else:
            self.__r_index = self.__original_r_index = None

        if write_value:
            self.__w_index = self.__original_w_index = index if (
                    w_index is None) else w_index
        else:
            self.__w_index = self.__original_w_index = None

        self.write_value = write_value
        self.read_value = read_value
        self.attr_name = attr_name

    def get_r_index(self, original: bool = False) -> Optional[int]:
        return self.__original_r_index if original else self.r_index

    def get_w_index(self, original: bool = False) -> Optional[int]:
        return self.__original_w_index if original else self.w_index

    @property
    def r_index(self):
        return self.__r_index

    @r_index.setter
    def r_index(self, value):
        if self.read_value:
            self.__r_index = value
        else:
            raise TypeError('Cannot set r_index.')

    @property
    def w_index(self):
        return self.__w_index

    @w_index.setter
    def w_index(self, value):
        if self.write_value:
            self.__w_index = value
        else:
            raise TypeError('Cannot set w_index.')


class MethodColumn(BaseColumn):
    """
    MethodColumn is for reserving column index.
    Use this column if you don't have to get value from instance but want to
    write value dynamically by using column_*.
    read -> get value from a cell.
    write -> return '' default. fix value by using column_*.
    """


class AttributeColumn(BaseColumn):
    """
    The purpose of AttributeColumn is to get a value from instance.
    attribute name must be equal to a class variable name of this column.
    <attr_name> = AttributeColumn(...)
    read -> get value from a cell.
    write -> return attr value from an instance.
    """
    def get_value_for_write(self, instance, **kwargs) -> Any:
        """
        instance: instance of class such as Django Model or Dataclass etc.
        attr_name: field name of Django Model
        """
        val = instance
        for attr_name in self.name.split('__'):
            val = getattr(val, attr_name)

        return val


class StaticColumn(BaseColumn):
    """
    StaticColumn returns a static value. The static value can define like
    StaitColumn(static_value=<static_value>) or
    ModelCsv.set_static_column(<column_name>, <static_value>)
    read -> return static value.
    write -> return static value.
    """
    is_static = True

    def __init__(self, *, static_value: Any = '', **kwargs):
        """
        static_value: the value always returned.
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
        if field not in self.read_values:
            raise NoSuchFieldError(
                f'`{self.model.__class__.__name__}` does not have a field '
                f'`{field}`')

        self.field_kwargs.update({field: value})

    @cached_property
    def read_values(self):
        return [f.name for f in self.model._meta.get_fields()]


class CsvPart:
    pass
