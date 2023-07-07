import collections
import dataclasses
import inspect
from typing import Any, Callable, MutableMapping, Type, TypeVar, Union

from .. import writers
from ..columns import (
    BaseForeignColumn,
    ColumnValidationError,
    ForeignAttributeColumn,
    ForeignMethodColumn,
    ForeignStaticColumn,
)
from ..exceptions import ValidationError
from ..utils import render_row

READ_PREFIX = "field_"
WRITE_PREFIX = "column_"


@dataclasses.dataclass
class ErrorMessage:
    name: str
    message: str
    row_number: int
    label: str = None
    column_index: int = None

    @property
    def is_none_field_error(self) -> bool:
        """True if error is raised in `field` method"""
        return self.name is None


class Row(MutableMapping):
    """
    Dict like object which contains results of `field_*` methods as key-value
    and ErrorMessage objects as list.
    e.g.
    if row.is_valid:
        Model.objects.create(**row)
    else:
        for error in row.errors:
            ...
    """

    def __init__(self, number: int, errors: list, values: dict):
        self.number = number  # Row number
        self.errors: list[ErrorMessage] = errors
        self._values = values

    def __repr__(self) -> str:
        return f"Row(number={self.number})"

    __str__ = __repr__

    def __getitem__(self, key):
        return self._values[key]

    def __delitem__(self, key):
        del self._values[key]

    def __iter__(self):
        if self.is_valid:
            return iter(self._values)
        return {}

    def __len__(self):
        return len(self._values)

    def __setitem__(self, key, value):
        self._values[key] = value

    def __add__(self, other) -> "Row":
        if not isinstance(other, Row):
            raise ValueError(
                f"unsupported operation type 'Row' and " f"'{other.__class__.__name__}'"
            )
        if other.number != self.number:
            raise ValueError(
                "Cannot join Rows each has a different number: " f"{self} and {other})"
            )
        values = self._values | other._values
        self.errors.extend(getattr(other, "errors"))
        return Row(number=self.number, errors=self.errors, values=values)

    @property
    def values(self) -> dict:
        if not self.is_valid:
            return {}

        return self._values

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def clean(self, exclude: list | None = None) -> None:
        exclude = exclude or []
        keys = list(self._values.keys())
        for key in keys:
            if key not in exclude:
                del self._values[key]

    def append_error(self, exception: ValidationError, name: str, row_number: int):
        self.errors.append(
            ErrorMessage(
                message=str(exception),
                name=name,
                row_number=row_number,
                label=exception.label,
                column_index=exception.column_index,
            )
        )


class TableForRead:
    class ReadIndexOverColumnNumberError(Exception):
        """
        check if csv has enough column number.
        I recommend to check headers before constructing ModelCsv instead of
        capture this exception.
        e.g)
        header, *table = reader.get_table()
        if header != ModelCsv._meta.get_headers(for_read=True):
            ...
        """

    def __init__(self, table: list[list]) -> None:
        self.table = table
        self._is_checked = False  # check if is_valid() called or not.
        self.validate()

    def validate(self):
        for col in self._meta.get_columns():
            col.validate_for_read()

        cols = self._meta.get_columns(for_read=True)
        if not cols:
            raise ColumnValidationError(
                f"{self.__class__.__name__} needs at least one column"
            )

        value_names = [col.value_name for col in cols if not col.is_relation]
        duplicates = [k for k, v in collections.Counter(value_names).items() if v > 1]
        if duplicates:
            raise ColumnValidationError(
                f"`value_name` must be unique. {duplicates} are duplicate."
            )

        indexes = self._meta.get_r_indexes()
        if len(indexes) != len(set(indexes)):
            raise ColumnValidationError(
                "`index` must be unique. Change `index` or `r_index`"
            )

        if len(self.table[0]) <= max(indexes):
            raise self.ReadIndexOverColumnNumberError(
                f"column number: {len(self.table[1])} < " f"r_index: {max(indexes)}"
            )

    def is_valid(self) -> bool:
        if self._is_checked:
            return self._is_valid

        self.__cleaned_rows: list[Row] = []
        for i, row in enumerate(self.table.copy()):
            row_model: Row = self.read_from_row(row, i)
            # if row raises any ValidationError, then `field` method is not
            # called because row.values may contain unexpected type.
            if row_model.is_valid:
                try:
                    row_model.update(
                        self.field(
                            values=row_model.values.copy(), static=self._static.copy()
                        )
                    )
                except ValidationError as e:
                    row_model.append_error(
                        exception=e,
                        row_number=i,
                        name=self.error_name_prefix + "field_method",
                    )
            self.__cleaned_rows.append(row_model)

        self._is_checked = True
        return self._is_valid

    @property
    def _is_valid(self) -> bool:
        return not bool(list(filter(lambda r: not r.is_valid, self.__cleaned_rows)))

    @property
    def cleaned_rows(self) -> list[Row]:
        if not self._is_checked:
            raise AttributeError(
                "cannot access `cleaned_rows` attribute "
                "before calling `is_valid()` method"
            )

        return self.__cleaned_rows


class RowForRead:
    """
    A class to read values from each row.
    1. get a value from csv. Call Column.get_value_for_read() method.
    2. change a type of the value from str to `column.to` type.
    3. apply all `field_*` method results.
    3.1 capture ValidationError if any.
    4. apply a `field` method result.
    """

    error_name_prefix = ""

    def field(self, values: dict, **kwargs):
        """
        The `values` are values fixed in `fields_*` methods.
        This method is not called if any `fields_*` method raises ValidationError.
        """
        return values

    def apply_method_change(self, values: dict, number: int) -> Row:
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
        errors = []
        for name, mthd in methods:
            if not name.startswith(READ_PREFIX):
                continue

            value_name = name.split(READ_PREFIX)[1]
            try:
                updated[value_name] = mthd(
                    values=values.copy(),
                    static=self._static.copy(),
                )
            except ValidationError as e:
                if e.column_index is None:
                    e.column_index = next(
                        (
                            col.r_index
                            for col in self._meta.get_columns(for_read=True)
                            if col.method_suffix == value_name
                        ),
                        None,
                    )
                errors.append(
                    ErrorMessage(
                        message=str(e),
                        name=self.error_name_prefix + value_name,
                        row_number=number,
                        label=e.label,
                        column_index=e.column_index,
                    )
                )

        return Row(number=number, errors=errors, values=updated)

    def read_from_row(
        self, row: list[str], number: int, is_relation: bool = False
    ) -> Row:
        """
        Make {column.value_name: value} dict to pass `field_*` methods and
        return a Row instance.
        """
        values = {
            col.value_name: self._meta.convert_from_str(
                col.get_value_for_read(row=row), to=col.to, column_index=col.r_index
            )
            for col in self._meta.get_columns(for_read=True, is_relation=is_relation)
        }

        return self.apply_method_change(values, number)


class CsvForRead(RowForRead, TableForRead):
    pass


class RowForWrite:
    def get_row_value(self, instance, is_relation: bool = False) -> dict[int, str]:
        """
        return {w_index: value}
        """

        def __get_row_from_column(_column):
            method_name = WRITE_PREFIX + _column.method_suffix
            if _column.has_callback:
                _value = _column.callback(
                    self, instance=instance, static=self._static.copy()
                )
            elif not _column.is_static and hasattr(self, method_name):
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
            row |= part.get_row_value(instance, is_relation=True)

        return row


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
        table = [self._meta.get_headers(for_write=True)] if header else []
        table += [
            render_row(
                self.get_row_value(instance=instance),
                insert_blank_column=self._meta.insert_blank_column,
            )
            for instance in self.instances
        ]

        return table

    def validate(self):
        for col in self._meta.get_columns():
            col.validate_for_write()

        if not self._meta.get_columns(for_write=True):
            raise ColumnValidationError(
                f"{self.__class__.__name__} needs at least one column"
            )

        index = self._meta.get_w_indexes()
        if len(index) != len(set(index)):
            raise ColumnValidationError(
                "`index` must be unique. Change `index` or `w_index`"
            )


BaseCsvType = TypeVar("BaseCsvType", bound="BaseCsv")


class BaseCsv:
    """
    Base Class to create CsvForRead and CsvForWrite.
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
            raise ValueError(f"`{column_name}` is not a static column.")

        column.static_value = value

    def set_static(self, key, value) -> None:
        self._static.update({key: value})

    @classmethod
    def for_read(cls, table: list[list]) -> BaseCsvType:
        if not cls._meta.read_mode:
            raise cls.ReadModeIsProhibited("Read Mode is prohibited")
        return type(
            f"{cls.__name__}ForRead", (cls, cls.read_class), {"_meta": cls._meta}
        )(table=table)

    @classmethod
    def for_write(cls, instances) -> BaseCsvType:
        if not cls._meta.write_mode:
            raise cls.WriteModeIsProhibited("Write Mode is prohibited")

        return type(
            f"{cls.__name__}ForWrite", (cls, cls.write_class), {"_meta": cls._meta}
        )(instances=instances)


class BaseModelRowForRead(RowForRead):
    """
    Base Class to run Part.get_part_of_row().
    Inherit this class if the class can be a Part class.
    """

    def read_from_row(
        self, row: list[str], number: int, is_relation: bool = False
    ) -> Row:
        row_model: Row = super().read_from_row(row, number, is_relation)
        for prt in self._meta.parts:
            row_model += prt.get_part_of_row(
                row=row, number=number, static=self._static.copy()
            )
        return row_model

    def remove_extra_values(self, values: dict) -> dict:
        return values


class PartForReadMixin:
    related_name: str

    @property
    def error_name_prefix(self):
        return f"{self.related_name}__"

    def get_part_of_row(self, row: list[str], number: int, static: dict) -> Row:
        self._static = static.copy()  # inject static from main csv.
        rw: Row = self.read_from_row(row, number, is_relation=True)
        if rw.is_valid:
            try:
                rw.update(self.field(values=rw.values))
            except ValidationError as e:
                rw.append_error(
                    exception=e,
                    name=self.error_name_prefix + "field_method",
                    row_number=number,
                )
            else:
                try:
                    rw[self.related_name] = self._callback(
                        values=rw.values.copy(), static=self._static.copy()
                    )
                except ValidationError as e:
                    rw.append_error(
                        exception=e,
                        name=self.error_name_prefix + "callback",
                        row_number=number,
                    )

        rw.clean(exclude=[self.related_name])
        return rw


class BasePartMixin:
    def __init__(
        self,
        related_name: str,
        callback: Union[str, Callable] = "get_or_create_object",
        **kwargs,
    ):
        self.related_name = related_name
        self._static = {}

        if isinstance(callback, str):
            self._callback = getattr(self, callback)
        elif callable(callback):
            self._callback = callback
        else:
            raise ValueError("`callback` must be str or callable.")
        # Don't call super().__init__ cos column validations should not run.

    def _add_column(
        self, column_class: Type[BaseForeignColumn], **kwargs
    ) -> BaseForeignColumn:
        column = column_class(related_name=self.related_name, **kwargs)
        self._meta.columns.append(column)
        return column

    # Use UpperCamel case.
    # e.g. prt.AttributeColumn()
    def AttributeColumn(self, attr_name: str, **kwargs):
        # attr_name is required
        return self._add_column(ForeignAttributeColumn, attr_name=attr_name, **kwargs)

    def MethodColumn(self, **kwargs):
        return self._add_column(ForeignMethodColumn, **kwargs)

    def StaticColumn(self, **kwargs):
        return self._add_column(ForeignStaticColumn, **kwargs)
