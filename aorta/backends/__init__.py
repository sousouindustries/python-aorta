import importlib
import inspect
import types

from aorta.backends import qpid_proton
from aorta.backends.base import BaseMessagingBackend
from aorta.backends.ireceiver import IReceiver
from aorta.backends.isender import ISender


def load(backend, *args, **kwargs):
    """Loads a messaging backend given its module or class name.

    Try to import `backend` as a module and get the `MessagingBackend`
    attribute. If the import fails, assume that it is a combination
    of module path/class name. If the import succeeds but the resulting
    attribute does not inherit from :class:`~aorta.backends.base.MessagingBackend`,
    raise a :exc:`TypeError`.

    If `backend` is a class and inherits from the base backend class,
    instantiate if. If `backend` is an instance of such a class, do
    nothing.

    The :func:`load` function will always return an instance of a
    backend.
    """
    INVALID_TYPE = TypeError("Invalid backend class.")
    if isinstance(backend, types.ModuleType):
        module = backend
        if not hasattr(module, 'MessagingBackend'):
            raise ImportError(
                "{0} does not specify a MessagingBackend class"
                    .format(backend)
            )
        if not inspect.isclass(module.MessagingBackend)\
        or not issubclass(module.MessagingBackend, BaseMessagingBackend):
            raise INVALID_TYPE

        backend = load(module.MessagingBackend, *args, **kwargs)
    elif isinstance(backend, str):
        try:
            module = importlib.import_module(backend)
            backend = load(module)
        except ImportError as e:
            # Assume module path/class name
            try:
                module_name, class_name = backend.rsplit('.', 1)
            except ValueError:
                raise e

            module = importlib.import_module(module_name)
            try:
                backend = load(getattr(module, class_name), *args, **kwargs)
            except AttributeError:
                raise ImportError("Module {0} has no attribute named {1}".format(module_name, class_name))
    elif inspect.isclass(backend):
        if not issubclass(backend, BaseMessagingBackend):
            raise INVALID_TYPE
        backend = backend(*args, **kwargs)
    elif isinstance(backend, BaseMessagingBackend):
        pass
    else:
        raise INVALID_TYPE

    return backend
