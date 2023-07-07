from copy import deepcopy

from ..base import BaseCsv
from .base import DjangoBasePart, DjangoCsvForRead
from .metaclasses import DjangoCsvMetaclass


class DjangoCsv(BaseCsv, metaclass=DjangoCsvMetaclass):
    read_class = DjangoCsvForRead

    @classmethod
    def as_part(
        cls, related_name: str, callback: str = "get_or_create_object"
    ) -> DjangoBasePart:
        _meta = deepcopy(cls._meta)
        _meta.as_part = True
        return type(f"{cls.__name__}Part", (cls, DjangoBasePart), {"_meta": _meta})(
            related_name=related_name, callback=callback
        )
