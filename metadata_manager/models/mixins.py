class ORMReprMixin:
    def __repr__(self) -> str:
        class_ = self.__class__.__name__
        attrs = sorted((k, getattr(self, k)) for k in self.__mapper__.columns.keys())  # type: ignore
        str_attrs = ",\n".join(f"{key}={value!r}" for key, value in attrs)
        return f"{class_}({str_attrs})"
