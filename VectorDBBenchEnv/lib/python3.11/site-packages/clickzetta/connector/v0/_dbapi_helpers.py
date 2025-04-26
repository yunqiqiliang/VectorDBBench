import functools

from clickzetta.connector.v0 import exceptions


def raise_on_closed(
    exc_msg, exc_class=exceptions.ProgrammingError, closed_attr_name="_closed"
):
    def _raise_on_closed(method):
        def with_closed_check(self, *args, **kwargs):
            if getattr(self, closed_attr_name):
                raise exc_class(exc_msg)
            return method(self, *args, **kwargs)

        functools.update_wrapper(with_closed_check, method)
        return with_closed_check

    def decorate_public_methods(klass):
        for name in dir(klass):
            if name.startswith("_") and name != "__iter__":
                continue

            member = getattr(klass, name)
            if not callable(member):
                continue

            if isinstance(klass.__dict__[name], (staticmethod, classmethod)):
                continue

            member = _raise_on_closed(member)
            setattr(klass, name, member)

        return klass

    return decorate_public_methods
