from typing import Any, List, Optional


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

    def get_value_for_write(self, instance, **kwargs):
        return ''


class ReadColumnMixin:
    def validate_for_read(self) -> None:
        # if static column, then an index is not necessary because this column
        # does not have to read from csv file, but return a same static value.
        if not self.read_value:
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

    def __init__(self, *, header: str = '', index: Optional[int] = None,
                 r_index: Optional[int] = None, w_index: Optional[int] = None,
                 read_value: bool = True, write_value: bool = True,
                 value_name: str = '', method_suffix: str = '', to: Any = str):
        """
        header: csv column header. <WRITE>
        index: csv column index. Start from 0. <READ, WRITE>
        r_index: csv column index. <READ>
        w_index: csv column index. <WRITE>
        read_value: if False then this column deos not pass any value. <READ>
        write_value: if False then this column does not write down any value. <WRITE>
        value_name: dict key of a value. <READ>
        method_suffix: suffix of special method column_<method_suffix>. <WRITE>
        to: Type of value. Used when convert value to str, or str to the type. <READ, WRITE>
        """
        self.__header = header

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
        self._value_name = value_name
        self._method_suffix = method_suffix
        self.to = to

        # column name is set in metaclasses.CsvOptions.
        self.name = None

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

    @property
    def header(self) -> str:
        return self.__header or self.name

    @property
    def method_suffix(self) -> str:
        """
        Mapping columns and results of special method `column_<method_suffix>`
        """
        return self._method_suffix or self.name

    @property
    def value_name(self) -> str:
        """
        Dict key of a value. This dict is passed to special method `field_*`
        """
        if self._value_name:
            return self._value_name

        return self.name


class MethodColumn(BaseColumn):
    """
    Use this column if you don't have to get value from instance but want to
    write value dynamically by using column_*.
    read -> get value from a cell.
    write -> return '' as default. Fix value by using column_*.
    """


class AttributeColumn(BaseColumn):
    """
    The purpose of AttributeColumn is to get a value from an instance.
    Attribute name must be equal to a class variable name of this column.
    <attr_name> = AttributeColumn(...)
    read -> get value from a cell.
    write -> return attr value from an instance.
    """

    def __init__(self, attr_name: str = '', **kwargs):
        """
        attr_name: an attribute name of an instance. <WRITE>
        """
        self.__attr_name = attr_name
        super().__init__(**kwargs)

    def get_value_for_write(self, instance, **kwargs) -> Any:
        """
        instance: An instance of class such as Django Model or Dataclass etc.
        attr_name: Field name of Django Model, or attribute name of an instance.
        """
        val = instance
        for attr_name in self.attr_name.split('__'):
            val = getattr(val, attr_name)

        return val

    @property
    def attr_name(self) -> str:
        """
        AttributeColumn use attr_name to get a value from a instance.
        """
        return self.__attr_name or self.name

    @property
    def method_suffix(self) -> str:
        """
        Mapping columns and results of special method `column_<method_suffix>`
        """
        return self._method_suffix or self.attr_name

    @property
    def value_name(self) -> str:
        """
        Dict key of a value. This dict is passed to special method `field_*`
        """
        if self._value_name:
            return self._value_name

        if self.is_relation:
            return self.attr_name

        return self.name


class StaticColumn(BaseColumn):
    """
    StaticColumn returns a static value. The static value can define like
    StaticColumn(static_value=<static_value>) or
    ModelCsv.set_static_column(<column_name>, <static_value>)
    read -> return a value from row.
    write -> return a static value.
    """
    is_static = True

    def __init__(self, *, static_value: Any = '', **kwargs):
        """
        static_value: the value always returned.
        """
        self.static_value = static_value
        super().__init__(**kwargs)

    def get_value_for_write(self, **kwargs) -> Any:
        return self.static_value


class BaseForeignColumn:
    """
    Column for ForeignKey model.
    """
    is_relation = True

    def __init__(self, field_name: str, **kwargs):
        """
        field_name: Field name of foreign model.
        attr_name: Attribute name of foreign model.
        e.g.
        class Child(models.Model):
            child_name = models.CharField(max_length=...)

        class Parent(models.Model):
            child = models.ForeignKey(Child, on_delete=...)

        `field_name` is 'child' and `attr_name` is 'child_name'.
        """
        self.__field_name = field_name

        super().__init__(**kwargs)

    @property
    def field_name(self) -> str:
        return self.__field_name


class ForeignMethodColumn(BaseForeignColumn, MethodColumn):
    def validate_for_read(self) -> None:
        super().validate_for_read()
        if self.value_name == self.name:
            raise ColumnValidationError(
                '`value_name` is required if `read_value` is True'
            )

    def validate_for_write(self) -> None:
        super().validate_for_read()
        if self.method_suffix == self.name:
            raise ColumnValidationError(
                '`method_suffix` is required if `write_value` is True'
            )


class ForeignAttributeColumn(BaseForeignColumn, AttributeColumn):

    def __init__(self, attr_name: str, **kwargs):
        if len(attr_name.split('__')) > 1:
            raise ValueError(f'`{attr_name}` is invalid attr name.'
                             'Don\'t include `__` in ForeignColumn attr_name.')

        super().__init__(attr_name=attr_name, **kwargs)

    @property
    def value_name(self) -> str:
        return self._value_name or self.attr_name


class ForeignStaticColumn(BaseForeignColumn, StaticColumn):
    def validate_for_read(self) -> None:
        super().validate_for_read()
        if self.value_name == self.name:
            raise ColumnValidationError(
                '`value_name` is required if `read_value` is True'
            )
