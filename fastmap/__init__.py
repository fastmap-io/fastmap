from .lib import (FastmapConfig, set_docstring, ExecPolicy, Verbosity, ExecutionError, RemoteError,
                  FASTMAP_DOCSTRING, INIT_DOCSTRING)

__version__ = "0.1.0"

_global_config = None

ExecPolicy = ExecPolicy
Verbosity = Verbosity
ExecutionError = ExecutionError
RemoteError = RemoteError

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
        return _global_config.fastmap(func, iterable)
    else:
        tmp_config = init(secret=None, exec_policy=ExecPolicy.LOCAL)
        tmp_config.log.warning("Fastmap not initialized. Defaulting to LOCAL exec_policy.")
        return tmp_config.fastmap(func, iterable)

def _reset_global_config():
    # For unit tests. Do not use
    global _global_config
    _global_config = None


# @set_docstring(FASTRUN_DOCSTRING)
# def fastrun(func, *args, **kwargs):
#     if _global_config:
#         _global_config.fastrun(func, *args, **kwargs)
#     else:
#         tmp_config = init(secret=None, exec_policy=ExecPolicy.LOCAL)
#         tmp_config.log.warning("Fastmap not initialized. Defaulting to LOCAL exec_policy.")
#         tmp_config.fastrun(func, *args, **kwargs)
