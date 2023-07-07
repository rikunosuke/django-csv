import dataclasses
from typing import TYPE_CHECKING, Type

from ...columns import AttributeColumn, BaseColumn
from ..metaclasses import BaseMetaclass, CsvOptions

if TYPE_CHECKING:
    from .base import DataClassBasePart


class DataClassOptions(CsvOptions):
    headers: dict[str, str]

    ALLOWED_META_ATTR = CsvOptions.ALLOWED_META_ATTR + (
        "dclass",
        "fields",
        "headers",
        "as_part",
    )

    def __init__(
        self,
        meta: type | None,
        columns: dict[str, BaseColumn],
        parts: list["DataClassBasePart"],
    ):
        if meta is None:
            raise ValueError("class `Meta` is required in `DataClassCsv`")

        if not hasattr(meta, "dclass"):
            raise ValueError("`dclass` is required in class `Meta.`")

        self.dclass: Type[dataclasses.dataclass] = meta.dclass

        if getattr(meta, "as_part", False):
            # DataClassCsvOptions of Part Class only has own relation columns.
            super().__init__(meta, {}, parts)
            return

        if not (field_names := getattr(meta, "fields", None)):
            super().__init__(meta, columns, parts)
            return

        # auto create AttributeColumns for fields.
        if field_names == "__all__":
            fields = [
                f
                for f in dataclasses.fields(self.dclass)
                if not dataclasses.is_dataclass(f.type)
            ]
        else:
            fields = [
                field
                for field in dataclasses.fields(self.dclass)
                if field.name in field_names
            ]

        # skip if the name is already used.
        column_names = columns.keys()
        fields = [f for f in fields if f.name not in column_names]

        _kwargs = {"columns": list(columns.values()), "original": True}

        unassigned_r = self.get_unassigned(
            [
                col.get_r_index(original=True)
                for col in self.filter_columns(r_index=True, **_kwargs)
            ]
        )

        unassigned_w = self.get_unassigned(
            [
                col.get_w_index(original=True)
                for col in self.filter_columns(w_index=True, **_kwargs)
            ]
        )

        for r, w, f in zip(unassigned_r, unassigned_w, fields):
            header = meta.headers.get(f.name) if hasattr(meta, "headers") else f.name

            to = f.type

            columns[f.name] = AttributeColumn(
                r_index=r, w_index=w, header=header, to=to
            )

        super().__init__(meta, columns, parts)


class DataClassMetaclass(BaseMetaclass):
    option_class = DataClassOptions
