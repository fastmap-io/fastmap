from .client_lib import (FastmapConfig, set_docstring, ExecPolicy, Verbosity,
                         EveryProcessDead, CloudError, CLIENT_VERSION, INIT_DOCSTRING,
                         GLOBAL_INIT_DOCSTRING, FASTMAP_DOCSTRING)

ExecPolicy = ExecPolicy
Verbosity = Verbosity
EveryProcessDead = EveryProcessDead
CloudError = CloudError
FastmapConfig = FastmapConfig

__version__ = CLIENT_VERSION
_global_config = None


@set_docstring(GLOBAL_INIT_DOCSTRING)
def global_init(*args, **kwargs):
    global _global_config
    _global_config = init(*args, **kwargs)


@set_docstring(INIT_DOCSTRING)
def init(*args, **kwargs):
    return FastmapConfig(*args, **kwargs)


@set_docstring(FASTMAP_DOCSTRING)
def fastmap(func, iterable):
    if _global_config:
        return _global_config.fastmap(func, iterable)
    else:
        tmp_config = init(exec_policy=ExecPolicy.LOCAL)
        tmp_config.log.warning("Fastmap not initialized globally."
                               "Defaulting to LOCAL exec_policy.")
        return tmp_config.fastmap(func, iterable)


def _reset_global_config():
    """ For unit tests. Do not use """
    global _global_config
    _global_config = None
