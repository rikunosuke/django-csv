from copy import deepcopy

from .base import DataClassCsvForRead, DataClassBasePart
from .metaclasses import DataClassMetaclass
from ..base import BaseCsv


class DataClassCsv(BaseCsv, metaclass=DataClassMetaclass):
    read_class = DataClassCsvForRead

    @classmethod
    def as_part(cls, related_name: str, callback: str = "get_dataclass") -> DataClassBasePart:
        _meta = deepcopy(cls._meta)
        _meta.as_part = True
        return type(
            f"{cls.__name__}Part", (cls, DataClassBasePart),
            {"_meta": _meta}
        )(related_name=related_name, callback=callback)
