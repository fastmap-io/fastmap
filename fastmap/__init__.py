from .sdk_lib import (FastmapConfig, set_docstring, ExecPolicy, Verbosity,
                      FastmapException, ReturnType, CLIENT_VERSION, INIT_DOCSTRING,
                      GLOBAL_INIT_DOCSTRING, FASTMAP_DOCSTRING, OFFLOAD_DOCSTRING,
                      POLL_DOCSTRING, POLL_ALL_DOCSTRING, KILL_DOCSTRING,
                      RESULT_DOCSTRING, CLEAR_DOCSTRING, CLEAR_ALL_DOCSTRING,
                      LOGS_DOCSTRING)

ExecPolicy = ExecPolicy
Verbosity = Verbosity
ReturnType = ReturnType
FastmapException = FastmapException
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


def _get_config():
    if _global_config:
        return _global_config
    tmp_config = init(exec_policy=ExecPolicy.LOCAL)
    tmp_config.log.warning("Fastmap not initialized globally."
                           "Defaulting to LOCAL exec_policy.")
    return tmp_config


@set_docstring(FASTMAP_DOCSTRING)
def fastmap(func, iterable, *args, **kwargs):
    return _get_config().fastmap(func, iterable, *args, **kwargs)


@set_docstring(OFFLOAD_DOCSTRING)
def offload(func, *args, **kwargs):
    return _get_config().offload(func, *args, **kwargs)


@set_docstring(POLL_DOCSTRING)
def poll(task_id):
    return _get_config().poll(task_id)


@set_docstring(POLL_ALL_DOCSTRING)
def poll_all():
    return _get_config().poll_all()


@set_docstring(KILL_DOCSTRING)
def kill(task_id):
    return _get_config().kill(task_id)


@set_docstring(RESULT_DOCSTRING)
def result(task_id):
    return _get_config().result(task_id)


@set_docstring(LOGS_DOCSTRING)
def logs(task_id):
    return _get_config().logs(task_id)


@set_docstring(CLEAR_DOCSTRING)
def clear(task_id):
    return _get_config().clear(task_id)


@set_docstring(CLEAR_ALL_DOCSTRING)
def clear_all(task_id):
    return _get_config().clear_all(task_id)


def _reset_global_config():
    """ For unit tests. Do not use """
    global _global_config
    _global_config = None
