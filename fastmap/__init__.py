import logging

from .lib import (ExecPolicy, FastmapConfig, set_docstring,
                  FASTMAP_DOCSTRING, FASTMAP_INIT_DOCSTRING)

_global_config = None


@set_docstring(FASTMAP_INIT_DOCSTRING, "Initialize fastmap globally. Also see fastmap_init.")
def fastmap_global_init(**kwargs):
    global _global_config
    _global_config = fastmap_init(**kwargs)


@set_docstring(FASTMAP_INIT_DOCSTRING, "Initialize a reusable FastmapConfig object with a .fastmap(...) method. Also see fastmap_global_init.")
def fastmap_init(**kwargs):
    return FastmapConfig(**kwargs)


@set_docstring(FASTMAP_DOCSTRING)
def fastmap(func, iterable, **kwargs):
    if _global_config:
        _global_config.fastmap(func, iterable)
    else:
        tmp_config = fastmap_init(exec_policy=ExecPolicy.LOCAL)
        tmp_config.log.warning("You have not initialized fastmap with fastmap_global_init. All processing will be performed on this machine.")
        tmp_config.fastmap(func, iterable)
