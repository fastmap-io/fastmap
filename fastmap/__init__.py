from .sdk_lib import (FastmapConfig, set_docstring, ExecPolicy, Verbosity, MachineType,
                      FastmapException, FastmapTask, TaskState, TaskOutcome,
                      CLIENT_VERSION, INIT_DOCSTRING, GLOBAL_INIT_DOCSTRING,
                      OFFLOAD_DOCSTRING, GET_TASK_DOCSTRING,
                      POLL_ALL_DOCSTRING, CLEAR_ALL_DOCSTRING)

# Clases
FastmapConfig = FastmapConfig
FastmapTask = FastmapTask

# Namespaces
ExecPolicy = ExecPolicy
Verbosity = Verbosity
MachineType = MachineType
TaskState = TaskState
TaskOutcome = TaskOutcome

# Exceptions
FastmapException = FastmapException


__version__ = CLIENT_VERSION
_global_config = None


@set_docstring(GLOBAL_INIT_DOCSTRING)
def global_init(*args, **kwargs):
    global _global_config
    _global_config = init(*args, **kwargs)


@set_docstring(INIT_DOCSTRING)
def init(*args, **kwargs):
    return FastmapConfig.create(*args, **kwargs)


def _get_config():
    if not _global_config:
        raise FastmapException("Fastmap not initialized globally.")
    return _global_config


# @set_docstring(MAP_DOCSTRING)
# def map(func, iterable, *args, **kwargs):
#     return _get_config().map(func, iterable, *args, **kwargs)


@set_docstring(OFFLOAD_DOCSTRING)
def offload(func, *args, **kwargs):
    return _get_config().offload(func, *args, **kwargs)


@set_docstring(POLL_ALL_DOCSTRING)
def poll_all():
    return _get_config().poll_all()


@set_docstring(CLEAR_ALL_DOCSTRING)
def clear_all():
    return _get_config().clear_all()


@set_docstring(GET_TASK_DOCSTRING)
def get_task(task_id):
    return _get_config().get_task(task_id)

# @set_docstring(POLL_DOCSTRING)
# def poll(task_id):
#     return _get_config().poll(task_id)


# @set_docstring(KILL_DOCSTRING)
# def kill(task_id):
#     return _get_config().kill(task_id)


# @set_docstring(RETURN_VALUE_DOCSTRING)
# def return_value(task_id):
#     return _get_config().return_value(task_id)


# @set_docstring(TRACEBACK_DOCSTRING)
# def traceback(task_id):
#     return _get_config().traceback(task_id)


# @set_docstring(WAIT_DOCSTRING)
# def wait(task_id, *args, **kwargs):
#     return _get_config().wait(task_id, *args, **kwargs)


# @set_docstring(ALL_LOGS_DOCSTRING)
# def all_logs(task_id, *args, **kwargs):
#     return _get_config().all_logs(task_id, *args, **kwargs)


# @set_docstring(CLEAR_DOCSTRING)
# def clear(task_id):
#     return _get_config().clear(task_id)


# @set_docstring(RETRY_DOCSTRING)
# def retry(task_id):
#     return _get_config().retry(task_id)


def _reset_global_config():
    """ For unit tests. Do not use """
    global _global_config
    _global_config = None
