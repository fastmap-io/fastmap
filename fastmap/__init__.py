from .lib import (FastmapConfig, set_docstring,
                  FASTMAP_DOCSTRING, INIT_DOCSTRING)

_global_config = None


@set_docstring(INIT_DOCSTRING, "Initialize fastmap globally. Also see init.")
def global_init(**kwargs):
    global _global_config
    _global_config = init(**kwargs)


@set_docstring(INIT_DOCSTRING, "Initialize a reusable FastmapConfig object. Also see global_init.")
def init(**kwargs):
    return FastmapConfig(**kwargs)


@set_docstring(FASTMAP_DOCSTRING)
def fastmap(func, iterable):
    if _global_config:
        _global_config.fastmap(func, iterable)
    else:
        tmp_config = init(secret=None, exec_policy='LOCAL')
        tmp_config.log.warning("Try initializing fastmap with global_init first.")
        tmp_config.fastmap(func, iterable)
