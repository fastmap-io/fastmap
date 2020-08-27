from .lib import (FastmapConfig, set_docstring, ExecPolicy, Verbosity,
                  ExecutionError, RemoteError, CLIENT_VERSION, INIT_DOCSTRING,
                  GLOBAL_INIT_DOCSTRING, FASTMAP_DOCSTRING)

ExecPolicy = ExecPolicy
Verbosity = Verbosity
ExecutionError = ExecutionError
RemoteError = RemoteError

__version__ = CLIENT_VERSION
_global_config = None

@set_docstring(GLOBAL_INIT_DOCSTRING)
def global_init(**kwargs):
    global _global_config
    _global_config = init(**kwargs)


@set_docstring(INIT_DOCSTRING)
def init(**kwargs):
    return FastmapConfig(**kwargs)


@set_docstring(FASTMAP_DOCSTRING)
def fastmap(func, iterable):
    if _global_config:
        return _global_config.fastmap(func, iterable)
    else:
        tmp_config = init(secret=None, exec_policy=ExecPolicy.LOCAL)
        tmp_config.log.warning("Fastmap not initialized globally."
                               "Defaulting to LOCAL exec_policy.")
        return tmp_config.fastmap(func, iterable)

def _reset_global_config():
    """ For unit tests. Do not use """
    global _global_config
    _global_config = None



