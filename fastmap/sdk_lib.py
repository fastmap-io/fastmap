"""
Primary file for the fastmap SDK. Almost all client-side code is in this file.
Do not instantiate anything here directly. Use the interface __init__.py.
"""

import atexit
import collections
import datetime
import distutils.sysconfig
import functools
import glob
import gzip
import hashlib
import hmac
import importlib.metadata
import json
import multiprocessing
import os
import pathlib
import queue
import re
import secrets
import sys
import threading
import traceback
import time
from collections.abc import Iterable, Sequence, Generator
from types import FunctionType, ModuleType
from typing import List, Dict

import dill
import msgpack
import requests

SECRET_LEN = 64
SECRET_RE = r'^[0-9a-f]{64}$'
TASK_RE = r'^[0-9a-f]{8}$'
SITE_PACKAGES_RE = re.compile(r".*?/python[0-9.]+/(?:site|dist)\-packages/")
REQUIREMENT_RE = re.compile(r'^[A-Za-z0-9_-]+==\d+(?:\.\d+)*$')
CLIENT_VERSION = "0.0.10"
KB = 1024
MB = 1024 ** 2
GB = 1024 ** 3

MAP_DOCSTRING = """
    Map a function over an iterable and return the results.
    Depending on prior configuration, fastmap will run either locally via
    multiprocessing, in the cloud on the fastmap.io servers, or adaptively on
    both.

    :param function func: Function to map against.
    :param sequence|generator iterable: Iterable to map over.
    :param dict kwargs: Named parameters to bind to the function. Optional.
    :param str return_type: Either "ELEMENTS" or "BATCHES". Default
        is "ELEMENTS".
    :param str label: Optional label to track this execution. Only meaningful if
        some execution occurs on the cloud. Default is "".
    :rtype: Generator

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in most circumstances.

    Notes:
    - The function passed in must be stateless and cannot access the network or
      the filesystem. If run locally, these restrictions will not be enforced
      but because fastmap will likely execute out-of-order, running stateful
      functions is not recommended.
    - The iterable can be a sequence (list, tuple, ndarray, dataframe, etc),
      or a generator.
    - Fastmap is a generator so the iterable is processed lazily and fastmap
      will not begin execution unless iterated over or execution is forced
      (e.g. by wrapping it in a list).

    For more documentation, go to https://fastmap.io/docs

    """

OFFLOAD_DOCSTRING = """
    Offload a function to the cloud and return a FastmapTask.

    :param function func: Function to offload
    :param dict kwargs: Named parameters to bind to the function. Optional.
    :param function hook: Function to call upon process completion. Optional.
    :param str label: Optional label to track this execution. Default is "".

    :rtype: FastmapTask
    """

POLL_ALL_DOCSTRING = """
    Poll all non-CLEARED cloud task metadata.

    :rtype: list[dict]
    """

POLL_DOCSTRING = """
    Poll for cloud task metadata.

    Raises a FastmapException if the task cannot be found.

    :param str task_id:
    :rtype: dict
    """

KILL_DOCSTRING = """
    Kill the associated cloud task.

    Raises a FastmapException if the task cannot be found or is already dead.

    :param str task_id:
    :rtype: None
    """

WAIT_DOCSTRING = """
    Block until the task completes. If the task is
    ultimately successful, return the function's return value.

    Raises a FastmapException if the result cannot be found,
    or if raise_exceptions is true and the task is not successful.

    :param str task_id:
    :param int polling_interval:
    :param bool live_logs:
    :param bool raise_exceptions
    :rtype: Various
    """

RETURN_VALUE_DOCSTRING = """
    Return the function's return value.

    Raises a FastmapException if the result cannot be found,
    if task has not completed, or if the task was not successful.

    :param str task_id:
    :rtype: Various
    """

TRACEBACK_DOCSTRING = """
    Return the traceback of an errored task.

    Raises a FastmapException if the result cannot be found,
    if task has not completed, or if the task did not error.

    :param str task_id:
    :rtype: str
    """

ALL_LOGS_DOCSTRING = """
    Return the task's stdout and stderr since the task started.

    Raises a FastmapException if the task cannot be found.

    :rtype: str
    """

NEW_LOGS_DOCSTRING = """
    Return the task's stdout and stderr since the task started.

    Raises a FastmapException if the task cannot be found.

    :rtype: str
    """

CLEAR_DOCSTRING = """
    Clear the task and remove its function, logs, and result from storage.

    Raises a FastmapException if the task cannot be found or the
    task has not completed.

    :param str task_id:
    :rtype: None
    """

RETRY_DOCSTRING = """
    Retry the task. Returns a new FastmapTask

    :param str task_id:
    :rtype: FastmapTask
    """

CLEAR_ALL_DOCSTRING = """
    Clear all done tasks and remove their functions, logs, and results from storage.
"""


INIT_PARAMS = """
    :param str|file|dict config: The json file path / dict of the
        configuration. Every subsequent argument will alter this configuration.
        This is optional and if empty, only subsequent parameters will be used.
    :param str secret: The API token generated on fastmap.io. Treat this like a
        password. Do not commit it to version control! Failure to do so could
        result in man-in-the-middle attacks or your credits being used by others
        (e.g. cryptocurrency miners). If None, fastmap will run locally
        regardless of exec_policy.
    :param str verbosity: 'SILENT', 'QUIET', 'NORMAL', or 'LOUD'.
        Default is 'NORMAL'.
    :param str exec_policy: 'LOCAL' or 'CLOUD'. Default is 'CLOUD'.
    :param str machine_type: 'CPU1', 'CPU7', or 'GPU7'. Only for the CLOUD exec_policy.
        Default is 'CPU1'.
    :param list requirements: A list of requirements in "package==1.2.3" style.
        If omitted, requirement discovery is automatic.

    For more documentation, go to https://fastmap.io/docs
    """

GLOBAL_INIT_DOCSTRING = """
    Initialize fastmap globally. All subsequent calls to fastmap will use this
    global configuration. Also see documentation for 'init'.
    %s
    :rtype: None

    Example usage:

        import fastmap

        fastmap.global_init(exec_policy="LOCAL", verbosity="LOUD")
        results = fastmap.fastmap(func, iterable)

    """ + INIT_PARAMS

INIT_DOCSTRING = """
    Create and return a FastmapConfig object. The FastmapConfig object has
    a method, fastmap, which can then be called. Also see documentation for
    'global_init'.
    %s
    :rtype: FastmapConfig

    Example usage:

        import fastmap

        fastmap_config = fastmap.init(machine_type="GPU", verbosity="QUIET")
        results = fastmap_config.fastmap(func, iterable)

    """ + INIT_PARAMS

dill.settings['recurse'] = True

# TODO
# Make dill.dumps deterministic https://github.com/uqfoundation/dill/issues/19
# This allows for hashing of the functions
# dill._dill._typemap = dict(sorted(dill._dill._typemap.items(),
#                                   key=lambda x: x[1]))

try:
    # Windows / mac use spawn. Linux uses fork. Set to spawn
    # because it has more issues and this provides a steady dev environment
    # Do not remove without at least adding more unit tests which operate
    # in a spawn environment
    multiprocessing.set_start_method("spawn")
except RuntimeError:
    pass


class Namespace(dict):
    """
    Abstract constants class
    Constants can be accessed via .attribute or [key] and can be iterated over.
    """
    def __init__(self, *args, **kwargs):
        d = {k: k for k in args}
        d.update(dict(kwargs.items()))
        super().__init__(d)

    def __getattr__(self, item):
        if item in self:
            return self[item]
        raise AttributeError


Verbosity = Namespace("SILENT", "QUIET", "NORMAL", "LOUD")
ExecPolicy = Namespace("LOCAL", "CLOUD")
AuthStatus = Namespace("AUTHORIZED")
InitStatus = Namespace("UPLOADED", "FOUND", "NOT_FOUND")
MapStatus = Namespace("NOT_FOUND", "BATCH_PROCESSED", "INITALIZING", "INITIALIZATION_ERROR", "PROCESS_ERROR")
DoneStatus = Namespace("DONE", "NOT_FOUND")
TaskState = Namespace("PENDING", "PROCESSING", "KILLING", "FINISHING", "DONE", "CLEARED")
TaskOutcome = Namespace("SUCCESS", "ERROR", "KILLED_BY_REQUEST", "KILLED_ZOMBIE")
MachineType = Namespace("CPU1", "CPU7", "GPU7")
Color = Namespace(
    GREEN="\033[92m",
    RED="\033[91m",
    YELLOW="\033[93m",
    CYAN="\033[36m",
    MAGENTA="\u001b[35m",
    CANCEL="\033[0m")


DEFAULT_INLINE_CONFIG = {
    'secret': None,
    'cloud_url': None,
    'verbosity': Verbosity.NORMAL,
    'exec_policy': ExecPolicy.CLOUD,
    'machine_type': MachineType.CPU1,
    'requirements': None,
}

DEFAULT_CONFIG_DIR = os.path.join(pathlib.Path.home(), '.fastmap', 'default_config.json')


def set_docstring(docstr: str, docstr_prefix='') -> FunctionType:
    """ Add the given doc string to each function """
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


def nowstamp() -> int:
    return datetime.datetime.now().timestamp()


def fmt_bytes(num_bytes: int) -> str:
    """
    Returns the human-readable byte quantity
    e.g. 2048 -> 2.0KB
    """
    if num_bytes >= GB:
        return "%.1fGB" % (num_bytes / GB)
    if num_bytes >= MB:
        return "%.1fMB" % (num_bytes / MB)
    if num_bytes >= KB:
        return "%.1fKB" % (num_bytes / KB)
    return "%dB" % num_bytes


def fmt_time(num_secs: int) -> str:
    """
    Returns a human-readable time scalar
    e.g. 121 -> 02:01
    """
    hours, remainder = divmod(num_secs, 3600)
    mins, secs = divmod(remainder, 60)
    if hours > 0:
        return '{:02}:{:02}:{:02}'.format(int(hours), int(mins), int(secs))
    if mins > 0:
        return '{:02}:{:02}'.format(int(mins), int(secs))
    return '{}s'.format(int(secs))


def fmt_dur(num_secs: int) -> str:
    """
    Returns a human-readable time scalar
    e.g. 121 -> 2.02 minutes
    """
    if num_secs >= 3600:
        return "%.2f hours" % (num_secs / 3600)
    if num_secs >= 60:
        return "%.2f minutes" % (num_secs / 60)
    if num_secs >= 1:
        return "%.2f seconds" % (num_secs)
    return "%d milliseconds" % (round(num_secs * 1000))


def get_credits(seconds: float, bytes_egress: float) -> float:
    """
    Estimate the number of credits spent.
    100 credits per vcpu hour + 100 credits per byte egress
    """
    return 8 * (seconds * 10.0 / 3600.0 + bytes_egress * 10.0 / GB)


def get_hash(binary: bytes) -> str:
    """
    Get the function hash for a dill pickled function.
    This is used mostly for caching and bucketing

    TODO:
    The problem is that dill is not deterministic so if we just take a
    hash of the pickled function, we will get different values each time.
    For now, this is fine because we are basically using the hash for the
    single run. In the future, we will need to make dill deterministic.

    To save time, don't try an approach with inspect.getsourcelines
    If upstream functions change, it won't capture the difference.

    Approach will be to find non-deteministic aspects of Python and replace
    them one-by-one in dill
    """
    return hashlib.sha256(binary).hexdigest()[:16]


class FastmapException(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """


class CloudError(FastmapException):
    """
    Thrown when something in a post request results in a non-200.
    Should be caught by anything that calls post_request

    The traceback (tb) can also be used to add more context to cloud errors
    """
    def __init__(self, *args, **kwargs):
        try:
            self.tb = kwargs.pop('tb')
        except KeyError:
            self.tb = None
        super().__init__(*args, **kwargs)


def simplified_tb():
    """
    Given a traceback, remove fastmap-specific lines (this file + dependencies)
    to make it easier for the end user to read and hide the sausage-making.
    To do so, go through a traceback line-by-line in reverse. The moment
    we have a fastmap-specific line, break and return
    """
    skip_dir_paths = (
        '  File "' + os.path.abspath(__file__),
        '/layers/google.python.pip/pip/',
    )
    tb_list = []
    tb_lines = traceback.format_exc().split('\n')
    preamble = tb_lines.pop()  # we want "Traceback (most rec..." no matter what
    for tb_line in reversed(tb_lines):
        if any(tb_line.startswith(path) for path in skip_dir_paths):
            # pop prev tb_line b/c each stack layer is 2 lines: file-loc & code
            tb_list.pop()
            break
        tb_list.append(tb_line)
    tb_list.append(preamble)

    return '\n'.join(reversed(tb_list)).strip()


def get_func_name(func: FunctionType) -> str:
    """Robust way to get the name of a random function"""
    try:
        name = func.__name__
    except AttributeError:
        name = repr(func)
    name = name[:40] + "..." if len(name) >= 45 else name
    return name


class FastmapLogger():
    """
    FastmapLogger exists primarily because it is difficult to pass python's
    native logger  between processes. Doing so was requiring a lot of
    weird workarounds. Otherwise, it should behave similarly
    """
    def __init__(self, verbosity: str):
        self.verbosity = verbosity
        self.restore_verbosity()

    def restore_verbosity(self):
        self.debug = self._debug
        self.info = self._info
        self.warning = self._warning
        self.error = self._error
        if self.verbosity == Verbosity.LOUD:
            pass
        elif self.verbosity == Verbosity.NORMAL:
            self.debug = self._do_nothing
        elif self.verbosity == Verbosity.QUIET:
            self.debug = self._do_nothing
            self.info = self._do_nothing
        elif self.verbosity == Verbosity.SILENT:
            self.debug = self._do_nothing
            self.info = self._do_nothing
            self.warning = self._do_nothing
        else:
            raise FastmapException(f"Unknown verbosity '{self.verbosity}'")

    def hush(self):
        self.debug = self._do_nothing  # noqa
        self.info = self._do_nothing  # noqa
        self.warning = self._do_nothing  # noqa
        self.error = self._do_nothing  # noqa

    def _do_nothing(self, *args):
        # This instead of a lambda because of pickling in multiprocessing
        pass

    def _debug(self, msg, *args):
        if args:
            msg = msg % args
        print("\033[K" + Color.CYAN + "fastmap DEBUG:" + Color.CANCEL, msg)

    def _info(self, msg, *args):
        if args:
            msg = msg % args
        print("\033[K" + Color.YELLOW + "fastmap INFO:" + Color.CANCEL, msg)

    def _warning(self, msg, *args):
        if args:
            msg = msg % args
        print("\033[K" + Color.RED + "fastmap WARNING:" + Color.CANCEL, msg)

    def _error(self, msg, *args):
        if args:
            msg = msg % args
        print("\033[K" + Color.RED + "fastmap ERROR:" + Color.CANCEL, msg, flush=True)

    def input(self, msg):
        # This exists mostly for test mocking
        return input(Color.CYAN + "\nfastmap: " + msg + Color.CANCEL)



# def local_worker_func(func: FunctionType, itdm: InterThreadDataManager,
#                       log: FastmapLogger) -> None:
#     """
#     A single persistent local worker. This function will process one
#     batch at a time until there are none left.
#     """
#     func = dill.loads(func)
#     try:
#         batch_tup = itdm.checkout()
#         while batch_tup:
#             batch_idx, batch_iter = batch_tup
#             start = time.perf_counter()
#             ret = list(map(func, batch_iter))
#             total_proc_time = time.perf_counter() - start
#             runtime = total_proc_time / len(ret)
#             log.debug("Batch %d local cnt=%d dur=%.2fs (%.2e/el).",
#                       batch_idx, len(ret), total_proc_time, runtime)
#             itdm.push_outbox(batch_idx, ret, runtime)
#             batch_tup = itdm.checkout()
#     except Exception as e:
#         proc_name = multiprocessing.current_process().name
#         itdm.put_error(proc_name, repr(e), batch_tup)
#         tb = simplified_tb(traceback.format_exc())
#         log.error("In local worker [%s]:\n %s.",
#                   multiprocessing.current_process().name, tb)
#         return


def auth_token(secret: str) -> str:
    """ The auth token is the first half of the secret """
    return secret[:SECRET_LEN // 2]


def sign_token(secret: str) -> str:
    """ The sign token is the first half of the secret """
    return secret[SECRET_LEN // 2:]


def hmac_digest(secret: str, payload: bytes) -> str:
    """ With our secret, generate a signature for the payload """
    return hmac.new(sign_token(secret).encode(), payload,
                    digestmod=hashlib.sha256).hexdigest()


def basic_headers(secret: str, payload: bytes) -> dict:
    """ Basic headers needed for most API calls. """
    return {
        'Authorization': 'Bearer ' + auth_token(secret),
        'X-Python-Version': sys.version.replace('\n', ''),
        'X-Client-Version': CLIENT_VERSION,
        'X-Content-Signature': hmac_digest(secret, payload),
        'X-Request-ID': secrets.token_hex()[:5],
    }


def post_request(url: str, data: dict, secret: str,
                 log: FastmapLogger) -> requests.Response:
    """
    Generic cloud post wrapper.
    This does warning/error management, extracts content, and checks signatures
    for every API post
    """
    if isinstance(data, dict):
        data = msgpack.dumps(data)

    headers = basic_headers(secret, data)
    log.debug("Posting to url %s with %s", url, fmt_bytes(len(data)))
    try:
        # TODO should this be in a backoff loop for robustness?
        # Would that make sense for all post requests??
        resp = requests.post(url, data=data, headers=headers)
    except requests.exceptions.ConnectionError:
        ping_url = url.split('/api')[0] + '/api/v1/ping'
        raise CloudError("Fastmap could not connect to %r. "
                         "Check your network connection. To check if your "
                         "server is running, try: `curl %s`." % (url, ping_url)) from None
    if 'X-Server-Warning' in resp.headers:
        # deprecations or anything else
        log.warning(resp.headers['X-Server-Warning'])

    if resp.status_code == 500:
        raise CloudError("Fastmap cloud error: %r" % resp.content)

    if resp.status_code == 401:
        # UNAUTHORIZED
        raise CloudError("Unauthorized. Check your API token.")

    if resp.status_code == 200 and \
       resp.headers.get('Content-Type') == 'application/msgpack':
        if 'X-Content-Signature' not in resp.headers:
            raise CloudError("Cloud payload was not signed (%d). "
                             "Will not unpickle." % (resp.status_code))
        cloud_hash = hmac_digest(secret, resp.content)
        if resp.headers['X-Content-Signature'] != cloud_hash:
            raise CloudError("Cloud checksum did not match. Will not unpickle.")
        resp.status = resp.headers['X-Status']
        log.debug("Response %s %s", resp.status_code, resp.status)
        try:
            resp.obj = msgpack.loads(resp.content)
        except msgpack.UnpicklingError:  # TODO might not be this exception
            raise CloudError("Error unpacking response") from None
        return resp

    raise CloudError("Unexpected Status / Content-Type %d (%s): %r" %
                     (resp.status_code,
                      resp.headers.get("Content-Type"),
                      resp.content[:100].strip()))


# def process_cloud_batch(itdm: InterThreadDataManager, batch_tup: tuple,
#                         map_url: str, func_hash: str, label: str,
#                         run_id: str, secret: str, log: FastmapLogger) -> None:
#     """
#     For /api/v1/map, finish preparing the request, send it, and handle the
#     response. Processed batches go back into the itdm. If a
#     processed batch leaves this function, it will end up back with the user.
#     """

#     start_req_time = time.perf_counter()

#     batch_idx, batch = batch_tup
#     try:
#         pickled_batch = dill.dumps(batch)
#     except Exception as ex:
#         raise CloudError("Could not pickle your data. "
#                          "Fastmap cannot run on the cloud.") from ex
#     compressed_batch = gzip.compress(pickled_batch, compresslevel=1)
#     payload = {
#         'func_hash': func_hash,
#         'batch': compressed_batch,
#         'label': label,
#         'run_id': run_id,
#     }

#     while True:
#         log.debug("Making cloud request batchlen=%d size=%s (%s/el)...",
#                   len(batch), fmt_bytes(len(compressed_batch)),
#                   fmt_bytes(len(compressed_batch) / len(batch)))
#         try TODO
#         resp = post_request(map_url, payload, secret, log)
#         if resp.status_code == 200:
#             if resp.status == MapStatus.INITALIZING:
#                 log.debug("Cloud worker is initializing. Last msg [%s]."
#                           " Retrying in 5 seconds..." %
#                           (resp.obj.get('init_step', '')))
#                 time.sleep(5)
#                 continue
#             elif resp.status == MapStatus.INITIALIZATION_ERROR:
#                 raise CloudError("Error initializing worker %r %r" % (
#                                  resp.obj.get('init_error'), resp.obj.get('init_tb')))
#         break

#     mem_used = (float(resp.headers.get('X-Mem-Used', 0.0))
#                 / float(resp.headers.get('X-Mem-Total', 1.0)))
#     if mem_used > 0.9:
#         log.warning("Cloud memory utilization high: %.2f%%. "
#                     "Consider increasing memory." % mem_used * 100)

#     if resp.status == MapStatus.BATCH_PROCESSED:
#         service_id = resp.headers['X-Service-Id']
#         total_request = time.perf_counter() - start_req_time
#         total_application = float(resp.headers['X-Application-Seconds'])
#         total_mapping = float(resp.headers['X-Map-Seconds'])
#         credits_used = float(resp.headers['X-Credits'])
#         result_len = len(resp.obj['results'])
#         req_time_per_el = total_request / result_len
#         app_time_per_el = total_application / result_len
#         map_time_per_el = total_mapping / result_len

#         log.debug("Batch %d cloud cnt=%d "
#                   "%.2fs/%.2fs/%.2fs map/app/req (%.2e/%.2e/%.2e per el) "
#                   "[%s].",
#                   batch_idx, result_len,
#                   total_mapping, total_application, total_request,
#                   map_time_per_el, app_time_per_el, req_time_per_el,
#                   service_id)
#         itdm.push_outbox(batch_idx,
#                          resp.obj['results'],
#                          None,
#                          credits_used=credits_used,
#                          network_seconds=total_request - total_application)
#         return

#     if resp.status == MapStatus.PROCESS_ERROR:
#         msg = "Your code could not be processed on the cloud: %s. " % \
#             resp.obj.get('exception')
#         bad_modules = resp.obj.get('bad_modules', [])
#         if bad_modules:
#             msg += "Modules with errors on import: %s." % ' '.join(bad_modules)
#             msg += "You might need to explicitly specify a requirements file " \
#                    "in your deployment."
#         raise CloudError(msg, tb=resp.obj.get('traceback', ''))
#     if resp.status == MapStatus.NOT_FOUND:
#         msg = "Your function was not found on the cloud."
#         raise CloudError(msg)
#     if resp.status_code == 402:
#         # NOT_ENOUGH_CREDITS
#         raise CloudError("Insufficient credits for this request. "
#                          "Your current balance is $%.4f." %
#                          resp.obj.get('credits_used', 0) / 100)
#     if resp.status_code == 403:
#         # INVALID_SIGNATURE
#         raise CloudError("Invalid signature. Check your token")
#     if resp.status_code == 410:
#         # DISCONTINUED (post-deprecated end-of-life)
#         raise CloudError("Fastmap.io API discontinued: %r" % resp.obj.get('reason'))
#     if resp.status_code == 413:
#         # TOO_LARGE
#         payload_len = len(msgpack.dumps(payload))
#         raise CloudError("Your request was too large (%s). "
#                          "Find a way to reduce the size of your data or "
#                          "function and try again." % fmt_bytes(payload_len))

#     if resp.status_code == 500 and resp.headers['Content-Type'] == 'text/html':
#         content = re.sub('<[^<]+?>', '', resp.text)
#         raise CloudError("Unexpected cloud error 500. You might have run out "
#                          "of memory. %s" % content)

#     # catch all (should just be for 500s of which a few are explicitly defined)
#     raise CloudError("Unexpected cloud response %d %s %r" %
#                      (resp.status_code, resp.status, resp.obj))


# def cloud_thread(thread_id: str, map_url: str, func_hash: str, label: str,
#                  run_id: str, itdm: InterThreadDataManager, secret: str,
#                  log: FastmapLogger):
#     """
#     A thread for running cloud requests in a loop. Batches are pulled out of
#     the itdm and passed into process_cloud_batch one-by-one until they are
#     exhausted. This also does some basic
#     """
#     batch_tup = itdm.checkout()
#     if batch_tup:
#         log.debug("Starting cloud thread %d []...", thread_id)
#     while batch_tup:
#         try:
#             process_cloud_batch(itdm, batch_tup, map_url, func_hash,
#                                 label, run_id, secret, log)
#         except CloudError as e:
#             proc_name = multiprocessing.current_process().name
#             thread_id = threading.get_ident()
#             error_loc = "%s: thread:%d" % (proc_name, thread_id)
#             itdm.put_error(error_loc, repr(e), batch_tup)
#             if hasattr(e, 'tb') and e.tb:
#                 tb = e.tb.replace('%0A', '\n')
#                 log.error("In cloud thread [%s]:\n%s.",
#                           threading.current_thread().name, tb)
#             else:
#                 log.error("In cloud thread [%s]: %r.",
#                           threading.current_thread().name, e)
#             log.error("Shutting down cloud thread [%s] due to error...",
#                       threading.current_thread().name)
#             return

#         batch_tup = itdm.checkout()


def get_modules(log: FastmapLogger) -> (Dict[str, str], List[ModuleType]):
    """
    Get in scope modules.
    Returns two things:
    1.  a dictionary of all mod_name -> source|None
    2.  a list of ModuleType for modules found in site packages
    For the former, a source is included if it is a local module. If it is
    an installed module, the source is None
    """
    std_lib_dir = os.path.realpath(distutils.sysconfig.get_python_lib(standard_lib=True))
    local_sources = {}
    installed_mods = []
    for mod_name, mod in sys.modules.items():
        if mod_name in sys.builtin_module_names:
            # builtin
            continue
        if mod_name.startswith("_"):
            # hidden
            continue
        if not getattr(mod, '__file__', None):
            # also not builtin
            continue
        mod_path = os.path.realpath(mod.__file__)
        if mod_path.startswith(std_lib_dir) and 'site-packages' not in mod_path:
            # not stdlib
            continue
        if hasattr(mod, "__package__") and \
           mod.__package__ in ("fastmap", "fastmap.fastmap"):
            # not fastmap
            continue

        # Through with the silent skips.
        # Looking for local_sources and installed_mods
        if SITE_PACKAGES_RE.match(mod.__file__):
            installed_mods.append(mod)
            continue
        if not mod.__file__.endswith('.py'):
            log.warning("The module %r is a non-Python locally-built module "
                        "which cannot be uploaded.", mod)
            continue
        with open(mod.__file__) as f:
            source = f.read()
            if source:
                local_sources[mod.__name__] = source

    return local_sources, installed_mods


def get_requirements(installed_mods: List[ModuleType],
                     log: FastmapLogger) -> List[str]:
    """
    TODO docstring
    """
    imported_module_names = set()
    site_packages_dirs = set()
    for mod in installed_mods:
        try:
            mod_name = mod.__package__
        except AttributeError:
            mod_name = None
        if not mod_name:
            # TODO
            # log.warning("Module %r had no __package__. If this causes problems, "
            #             "try using the 'requirements' parameter.", mod.__name__)
            mod_name = mod.__name__
        imported_module_names.add(mod_name)
        site_packages_dirs.add(SITE_PACKAGES_RE.match(mod.__file__).group(0))

    top_level_files = set()
    for site_packages_dir in site_packages_dirs:
        top_level_path = site_packages_dir + "*.dist-info/top_level.txt"
        top_level_files.update(glob.glob(top_level_path))

    packages_by_module = collections.defaultdict(set)
    for fn in top_level_files:
        with open(fn) as f:
            modules = f.read().split('\n')
        metadata_fn = fn.rsplit('/', 1)[0] + '/METADATA'
        pkg_name = None
        with open(metadata_fn) as f:
            for row in f.readlines():
                if match := re.match(r"Name: (?P<pkg_name>[a-zA-Z0-9-]+)", row):
                    pkg_name = match['pkg_name']
                    break
        if not pkg_name:
            raise FastmapException("No package name for %r" % fn)
        for mod_name in modules:
            packages_by_module[mod_name].add(pkg_name)

    requirements = {}
    missed_modules = set()
    for mod_name in imported_module_names:
        pkg_names = packages_by_module[mod_name]
        if not pkg_names:
            # log.warning("Could not find version for module %r. Skipping...",
            #             mod_name)
            missed_modules.add(mod_name)
            # requirements[mod_name] = None
            continue
        for pkg_name in pkg_names:
            pkg_version = importlib.metadata.version(pkg_name)
            requirements[pkg_name] = pkg_version

    # one last run-through to make sure we didn't forget anything
    # this fixed the issue with google.cloud.vision import
    for missed_mod in missed_modules:
        missed_mod = missed_mod.replace('.', '-')
        try:
            pkg_version = importlib.metadata.version(missed_mod)
        except:
            continue
        requirements[missed_mod] = pkg_version

    return sorted([f'{k}=={v}' for k, v in requirements.items()])


def get_dependencies(requirements: dict, log: FastmapLogger) -> (dict, dict):
    """
    Get dependency dictionary.
    Keys are module names.
    Values are either pip version strings or source code.
    """
    local_sources, installed_mods = get_modules(log)
    log.debug("Found %d installed modules" % len(installed_mods))
    log.debug("Found local imports %r" % list(sorted(local_sources.keys())))

    if requirements:
        log.debug("Skipping requirements autodetect.")
    else:
        requirements = get_requirements(installed_mods, log)
        log.info("Autodetected requirements %r." % requirements)

    installed_mods = [im.__name__ for im in installed_mods]
    return local_sources, installed_mods, requirements


def seq_batcher(sequence: Sequence, size: int) -> Generator:
    seq_len = len(Sequence)
    for idx in range(0, seq_len, size):
        yield sequence[idx:min(idx + size, seq_len)]


class AuthCheck(threading.Thread):
    """
    Thread to check the authorization status. This allows us to kill the
    process before packaging and sending up a potentially large payload
    """
    def __init__(self, config, *args, **kwargs):
        self.secret = config.secret
        self.url = config.cloud_url
        self.log = config.log
        super().__init__(*args, **kwargs)

    def run(self):
        payload = msgpack.dumps({})
        try:
            resp = post_request(self.url + '/api/v1/auth', payload,
                                self.secret, self.log)
        except CloudError as ex:
            self.log.error("During auth check [%s]:\n%s.",
                           multiprocessing.current_process().name, ex)
            self.success = False
            return
        if resp.status_code == 200 and resp.status == AuthStatus.AUTHORIZED:
            self.log.info("Authentication for %r was successful.", self.url)
        else:
            self.log.warning("Authentication failed for %r %r. Check your token.",
                             self.url, resp.status)
            self.success = False
        self.success = True

    def was_success(self):
        # exists for mocks
        return self.success


def pickle_function(func, func_name):
    try:
        return dill.dumps(func, recurse=True)
    except Exception as ex:
        err = "Your function %r could not be pickled." % func_name
        raise FastmapException(err) from ex


class HeartbeatIO():
    def __init__(self, logs_queue, heartbeat_queue):
        self.logs_queue = logs_queue
        self.heartbeat_queue = heartbeat_queue
        self.is_open = True

    def write(self, s):
        self.logs_queue.put(s)
        self.heartbeat_queue.put(nowstamp())
        return len(s)

    def flush(self):
        pass

    def close(self):
        self.is_open = False


class RedirectStdStreams(object):
    def __init__(self, heartbeat_io):
        self.heartbeat_io = heartbeat_io

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout = self.heartbeat_io
        sys.stderr = self.heartbeat_io

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr


def heartbeat_loop(heartbeat_queue, kill_queue):
    last_send = None
    while True:
        try:
            kill_queue.get(block=False)
            break
        except queue.Empty:
            pass
        if not last_send or nowstamp() - last_send > 60:
            heartbeat_queue.put(nowstamp())
            last_send = nowstamp()
        time.sleep(1)


def local_offload_wrapper(func_payload, result_queue, logs_queue, heartbeat_queue):
    start_time = nowstamp()
    func_dict = msgpack.loads(gzip.decompress(func_payload))
    heartbeat_io = HeartbeatIO(logs_queue, heartbeat_queue)
    kill_queue = multiprocessing.Queue()
    heartbeat_thread = threading.Thread(target=heartbeat_loop, args=(heartbeat_queue, kill_queue))
    heartbeat_thread.start()

    with RedirectStdStreams(heartbeat_io):
        try:
            func = dill.loads(func_dict['func'])
            ret = func()
            result_dict = {
                'outcome': 'SUCCESS',
                'return_value': ret,
                'exception': None,
                'traceback': None,
            }
            pickled_result = dill.dumps(result_dict)
        except Exception as ex:
            result_dict = {
                'outcome': 'ERROR',
                'return_value': None,
                'exception': repr(ex),
                'traceback': simplified_tb(),
            }
            pickled_result = dill.dumps(result_dict)
    kill_queue.put(True)
    runtime = nowstamp() - start_time
    result_queue.put((pickled_result, runtime))


# def local_map_wrapper(func_payload, pickled_iterable, result_queue, logs_queue):
#     func = dill.loads(pickled_func)
#     iterable = dill.loads(pickled_iterable)
#     logs = HeartbeatIO(logs_queue)
#     start_time = datetime.datetime.now()
#     with contextlib.redirect_stderr(logs):
#         with contextlib.redirect_stdout(logs):
#             try:
#                 ret = list(map(func, iterable))   # TODO multiprocessing.Pool
#                 resp = {
#                     'outcome': 'SUCCESS',
#                     'return_value': ret,
#                     'exception': None,
#                     'tb': None,
#                 }
#             except Exception as ex:
#                 resp = {
#                     'outcome': 'ERROR',
#                     'return_value': None,
#                     'exception': repr(ex),
#                     'tb': simplified_tb(),
#                 }
#     runtime = (datetime.datetime.now() - start_time).total_seconds()
#     result_queue.put((dill.dumps(resp), runtime))


def task_hook_thread(task, hook):
    try:
        ret = task.wait()
    except:
        return
    hook(ret)


OfldStatus = Namespace("NOT_FOUND", "ACKNOWLEDGED", "ERROR")


class FastmapTask():
    POLLING_INTERVAL = 3

    def __repr__(self):
        return "<%s id=%s state=%s outcome=%s>" % (
            self.__class__.__name__, self.task_id, self._task_state, self._outcome)

    def add_hook(self, hook):
        t = threading.Thread(target=task_hook_thread, args=(self, hook))
        t.start()

    def wait(self, polling_interval=None, live_logs=False, raise_exceptions=False):
        # TODO should this throw an error if it doesn't work? Or maybe we
        # shouldn't return results at all?
        def handle_anomaly(msg):
            if raise_exceptions:
                raise FastmapException(msg)
            self._config.log.info(msg)

        self._config.log.info("Waiting for task to finish...")

        while True:
            if live_logs:
                logs = self.new_logs()
                if logs:
                    sys.stdout.write("\033[K" + Color.MAGENTA + logs + Color.CANCEL)
            else:
                self.poll()
            if self._outcome == TaskOutcome.SUCCESS:
                return self.return_value()
            if self._outcome == TaskOutcome.ERROR:
                tb = self.traceback()
                handle_anomaly("Task error %r" % self._result_dict['exception'])
                print(tb)
                return
            if self._outcome in (TaskOutcome.KILLED_BY_REQUEST, TaskOutcome.KILLED_ZOMBIE):
                handle_anomaly("Task was killed")
                return
            if self._task_state == TaskState.CLEARED:
                handle_anomaly("Task has been cleared")
                return
            if self._task_state == TaskState.KILLING:
                handle_anomaly("Task is being killed")
                return
            time.sleep(polling_interval or self.POLLING_INTERVAL)

    def traceback(self):
        self._fetch_result_dict()
        if self._task_state == TaskState.CLEARED:
            raise FastmapException("Traceback cannot be retrieved because task is cleared.")
        if self._task_state != TaskState.DONE:
            raise FastmapException("Traceback cannot be retrieved because task is not done.")
        if self._outcome != TaskOutcome.ERROR:
            raise FastmapException("Traceback cannot be retrieved because task did not error.")
        return self._result_dict['traceback']

    def return_value(self):
        self._fetch_result_dict()
        if self._task_state == TaskState.CLEARED:
            raise FastmapException("Return value cannot be retrieved because task is cleared.")
        if self._task_state != TaskState.DONE:
            raise FastmapException("Return value cannot be retrieved because task is not done.")
        if self._outcome != TaskOutcome.SUCCESS:
            raise FastmapException("Return value cannot be retrieved because task did not succeed.")
        return self._result_dict['return_value']

    def new_logs(self):
        new_logs = self._fetch_logs()
        if self._task_state == TaskState.CLEARED:
            raise FastmapException("Logs cannot be retrieved because task is cleared.")
        return new_logs

    def all_logs(self):
        self._fetch_logs()
        if self._task_state == TaskState.CLEARED:
            raise FastmapException("Logs cannot be retrieved because task is cleared.")
        return self._all_logs


class FastmapLocalTask(FastmapTask):
    def __init__(self, config, func_name, task_type, proc=None, result_queue=None,
                 logs_queue=None, heartbeat_queue=None, label=''):
        self.task_id = secrets.token_hex()[:8]
        self.task_type = task_type
        self._config = config
        self._task_state = TaskState.PENDING
        self._func_name = func_name
        self._label = label
        self._outcome = None
        self._result_dict = None
        self._runtime = None
        self._heartbeat_ts = None

        self._proc = proc
        self._result_queue = result_queue
        self._logs_queue = logs_queue
        self._heartbeat_queue = heartbeat_queue
        self._all_logs = ""
        self._starttime = nowstamp()

    @staticmethod
    def create(config, func_payload, func_name, hook, label):
        result_queue = multiprocessing.Queue()
        logs_queue = multiprocessing.Queue()
        heartbeat_queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=local_offload_wrapper,
                                       args=(func_payload, result_queue,
                                             logs_queue, heartbeat_queue))
        try:
            proc.start()
        except RuntimeError:
            raise FastmapException("Error starting local process. It's likely "
                                   "that you need to wrap your code in an "
                                   "`if __name__ == '__main__'` context.")
        fp = FastmapLocalTask(config, func_name, "OFFLOAD", proc=proc,
                              result_queue=result_queue,
                              logs_queue=logs_queue, heartbeat_queue=heartbeat_queue,
                              label=label)
        if hook:
            fp.add_hook(hook)
        return fp

    @staticmethod
    def create_map(config, func_payload, func_name, iterable, hook, label):
        pickled_iterable = dill.dumps(iterable)
        result_queue = multiprocessing.Queue()
        logs_queue = multiprocessing.Queue()
        heartbeat_queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=local_map_wrapper,
                                       args=(func_payload, pickled_iterable,
                                             result_queue, logs_queue, heartbeat_queue)) # TODO heartbeat_queue
        try:
            proc.start()
        except RuntimeError:
            raise FastmapException("Error starting local process. It's likely "
                                   "that you need to wrap your code in an "
                                   "`if __name__ == '__main__'` context.")
        fp = FastmapLocalTask(config, func_name, "MAP", proc=proc,
                              result_queue=result_queue,
                              logs_queue=logs_queue, heartbeat_queue=heartbeat_queue,
                              label=label)
        if hook:
            fp.add_hook(hook)
        return fp

    def poll(self):
        try:
            pickled_result, self._runtime = self._result_queue.get(block=False)
            if not self._task_state == TaskState.CLEARED:
                self._task_state = TaskState.DONE
            self._result_dict = dill.loads(pickled_result)
            self._outcome = self._result_dict['outcome']
        except queue.Empty:
            pass

        while True:
            try:
                self._heartbeat_ts = self._heartbeat_queue.get(block=False)
            except queue.Empty:
                break

        return {
            'type': self.task_type,
            'func_name': self._func_name,
            "task_id": self.task_id,
            "task_state": self._task_state,
            'outcome': self._outcome,
            'start_time': datetime.datetime.fromtimestamp(self._starttime),
            'runtime': self._runtime,

            'label': self._label,
            'last_heartbeat': nowstamp() - self._heartbeat_ts if self._heartbeat_ts else None,
            'items_uploaded': None,  # TODO for map items
            'items_completed': None,  # TODO for map items
        }

    def kill(self):
        self._config.log.info("Killing task %s...", self.task_id)
        self._task_state = TaskState.KILLING
        self._proc.kill()
        self._outcome = TaskOutcome.KILLED_BY_REQUEST
        self._task_state = TaskState.DONE
        self.poll()

    def retry(self):
        # TODO
        pass

    def _fetch_logs(self):
        self.poll()
        new_logs = ''
        while True:
            try:
                new_logs += self._logs_queue.get(block=False)
            except queue.Empty:
                break
        self._all_logs += new_logs
        return new_logs

    def clear(self):
        if not self._task_state == TaskState.DONE:
            raise FastmapException("Task not done")
        self._task_state = TaskState.CLEARED
        self._config.log.info("Clearing task %s...", self.task_id)
        self.poll()

    def _fetch_result_dict(self):
        if self._result_dict:
            return
        self.poll()


class FastmapCloudTask(FastmapTask):
    def __init__(self, config, task_id=None):
        self.task_id = task_id
        self._config = config
        self._task_state = TaskState.PENDING
        self._outcome = None
        self._next_log_idx = 0
        self._all_logs = ''
        self._logs_done = False
        self._result_dict = None

    @staticmethod
    def create(config, func_name, func_hash, hook, label):
        url = config.cloud_url + "/api/v1/offload"
        payload = {
            "func_name": func_name,
            "func_hash": func_hash,
            "label": label,
            "machine_type": config.machine_type,
        }
        config.log.info("Starting new task for function %r..." % func_name)
        try:
            resp = post_request(url, payload, config.secret, config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status in (OfldStatus.ERROR, OfldStatus.NOT_FOUND):
            raise FastmapException("Internal cloud error. Try again later.")

        if resp.status == OfldStatus.ACKNOWLEDGED:
            task_id = resp.obj['task']['task_id']
            task = FastmapCloudTask(config, task_id=task_id)
            config.log.info("Created new task %r." % task)
            if hook:
                task.add_hook(hook)
            return task
        raise FastmapException("Got unexpected response from server %r" % resp.status)

    @staticmethod
    def create_map(config, func_name, func_hash, iterable, kwargs, hook, label):
        url = config.cloud_url + "/api/v1/map"
        payload = {
            "func_name": func_name,
            "func_hash": func_hash,
            "kwargs": kwargs,
            "label": label,
        }
        config.log.debug("Calling /api/v1/map")

        assert isinstance(iterable, list)  # TODO
        assert not hook  # TODO
        task_id = None
        page_offset = 0
        for i, batch in enumerate(seq_batcher(iterable, 10)):  # TODO
            config.log.info("Uploading batch %d" % i)  # TODO
            payload['page_idx'] = i
            payload['page_len'] = len(batch)
            payload['page_offset'] = page_offset
            payload['iterable'] = dill.dumps(batch)
            payload['task_id'] = task_id
            page_offset += len(batch)
            try:
                resp = post_request(url, payload, config.secret, config.log)
            except CloudError as ex:
                # TODO
                raise ex
            if resp.status in (OfldStatus.ERROR, OfldStatus.NOT_FOUND):
                raise FastmapException("Internal cloud error. Try again later.")
            if task_id:
                assert resp.obj['task']['task_id'] == task_id
            else:
                task_id = resp.obj['task']['task_id']

        return FastmapCloudTask(config, "MAP", task_id=task_id)
        raise FastmapException("Got unexpected response from server %r" % resp.status)  # TODO

    def poll(self):
        self._config.log.debug("Calling /api/v1/poll")
        url = self._config.cloud_url + "/api/v1/poll"
        payload = {"task_id": self.task_id}
        try:
            resp = post_request(url, payload, self._config.secret, self._config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status == 'NOT_FOUND':
            raise FastmapException("No task found")
        if resp.status == 'FOUND':
            task_dict = resp.obj['task']
            make_dt(task_dict)
            self._task_state = resp.obj['task']['task_state']
            self._outcome = resp.obj['task']['outcome']
            return task_dict
        raise FastmapException("Unexpected status from server %r" % resp.status)

    def kill(self):
        self._config.log.debug("Calling /api/v1/kill")
        url = self._config.cloud_url + "/api/v1/kill"
        payload = {"task_id": self.task_id}
        try:
            resp = post_request(url, payload, self._config.secret, self._config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status == 'NOT_FOUND':
            raise FastmapException("No task found")
        if resp.status == 'FOUND':
            self._config.log.info("Server acknowledged kill order for task %s.", self.task_id)
            task_dict = resp.obj['task']
            make_dt(task_dict)
            self._task_state = task_dict['task_state']
            self._outcome = task_dict['outcome']
            return task_dict
        raise FastmapException("Unexpected status from server %r" % resp.status)

    def _fetch_logs(self):
        if self._logs_done:
            return ""
        self._config.log.debug("Calling /api/v1/logs")
        url = self._config.cloud_url + "/api/v1/logs"
        payload = {"task_id": self.task_id, "next_log_idx": self._next_log_idx}
        try:
            resp = post_request(url, payload, self._config.secret, self._config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status == 'NOT_FOUND':
            raise FastmapException("No task found")
        if resp.status == 'FOUND':
            self._next_log_idx = resp.obj['next_log_idx']
            self._all_logs += resp.obj['logs'].decode()
            task_dict = resp.obj['task']
            self._task_state = task_dict['task_state']
            self._outcome = task_dict['outcome']
            if self._task_state in (TaskState.DONE, TaskState.CLEARED):
                self._logs_done = True
            return resp.obj['logs'].decode()
        raise FastmapException("Unexpected status from server %r" % resp.status)

    def retry(self):
        self._config.log.debug("Calling /api/v1/retry")
        url = self._config.cloud_url + '/api/v1/retry'
        payload = {"task_id": self.task_id}
        try:
            resp = post_request(url, payload, self._config.secret, self._config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status != 'ACKNOWLEDGED':
            # TODO more error handling
            raise FastmapException("Server error doing retry")
        new_task_dict = resp.obj['task']
        new_task_id = new_task_dict['task_id']
        self._config.log.info("Server is retrying task %s with new task %s" % (self.task_id, new_task_id))
        # TODO hook for new retry
        return FastmapCloudTask(self._config, task_id=new_task_id)

    def clear(self):
        self._config.log.debug("Calling /api/v1/clear")
        url = self._config.cloud_url + "/api/v1/clear"
        payload = {"task_id": self.task_id}
        try:
            resp = post_request(url, payload, self._config.secret, self._config.log)
        except CloudError as ex:
            # TODO
            raise ex
        if resp.status == 'NOT_FOUND':
            raise FastmapException("No task found")
        if resp.status == 'NOT_READY':
            raise FastmapException("Task not cleared. Task status is not \"DONE\".")
        if resp.status == 'FOUND':
            self._config.log.info("Server cleared task %s...", self.task_id)
            task_dict = resp.obj['task']
            make_dt(task_dict)
            self._task_state = task_dict['task_state']
            self._outcome = task_dict['outcome']
            return task_dict
        raise FastmapException("Unexpected status from server %r" % resp.status)

    def _fetch_result_dict(self):
        if self._result_dict:
            return
        url = self._config.cloud_url + "/api/v1/result"
        payload = {"task_id": self.task_id}

        result_idx = 0
        result_buffer = b''
        while True:
            self._config.log.debug("Calling /api/v1/result part %d" % result_idx)
            payload['result_idx'] = result_idx
            try:
                resp = post_request(url, payload, self._config.secret, self._config.log)
            except CloudError as ex:
                # TODO
                raise ex
            if resp.status == 'NOT_FOUND':
                raise FastmapException("No task found")

            task_dict = resp.obj['task']
            self._task_state = task_dict['task_state']
            self._outcome = task_dict['outcome']

            if resp.status == 'NOT_READY':
                break
                # raise FastmapException("Result not ready")
            if resp.status not in ('ERROR', "SUCCESS"):
                break
                # raise FastmapException("Unexpected status from server %r" % resp.status)

            if resp.obj['instruction'] == 'APPEND':
                result_buffer += resp.obj['result_part']
                result_idx += 1
                assert result_idx < resp.obj['result_len']
                continue

            assert resp.obj['instruction'] == 'UNPICKLE'
            result_buffer += resp.obj['result_part']
            try:
                self._result_dict = dill.loads(gzip.decompress(result_buffer))
            except dill.UnpicklingError:
                raise FastmapException("Error unpickling response") from None
            except gzip.BadGzipFile:
                raise FastmapException("Error unzipping response") from None

            # if self._task_type == 'MAP':
            #     self._result_dict += _return
            # else:
            #     self._result_dict = _return
            break


def local_exit_handler(config):
    # config.log.info("Received exit signal. Would kill %d local task(s)... " % len(config.local_threads))
    # for thread in config.local_threads:
    #     thread.kill()
    pass


class FastmapConfig():
    """
    The configuration object. Do not instantiate this directly.
    Instead, either:
    - use init to get a new FastmapConfig object
    - use global_init to allow fastmap to run without an init object.

    This object exposes one public method: fastmap.
    """

    __slots__ = [
        "secret",
        "verbosity",
        "log",
        "exec_policy",
        "machine_type",
        "cloud_url",
        "requirements",
        "local_threads",
    ]

    def __init__(self, config):
        # TODO parameter checking is weirdly divided between create and init
        self.exec_policy = config['exec_policy']
        self.log = FastmapLogger(config['verbosity'])
        self.verbosity = config['verbosity']
        self.cloud_url = config['cloud_url']
        self.requirements = config['requirements']
        self.machine_type = config['machine_type']
        self.local_threads = []

        if self.cloud_url:
            if not self.cloud_url.startswith("http"):
                self.cloud_url = "http://" + self.cloud_url
            if self.cloud_url.endswith("/"):
                self.cloud_url = self.cloud_url[:-1]
        elif self.exec_policy != ExecPolicy.LOCAL:
            self.exec_policy = ExecPolicy.LOCAL
            self.log.warning("No cloud_url provided. "
                             "Setting exec_policy to LOCAL.")

        if multiprocessing.current_process().name != "MainProcess":
            # Fixes issue with multiple loud inits during local multiprocessing
            # in Mac / Windows
            self.log.hush()

        if config['secret']:
            if not isinstance(config['secret'], str) or not re.match(SECRET_RE, config['secret']):
                raise FastmapException("Invalid secret token format.")
            self.secret = config['secret']
        else:
            if self.exec_policy != ExecPolicy.LOCAL:
                raise FastmapException("No secret provided on exec_policy==LOCAL.")
            self.secret = None

        if self.requirements:
            if not isinstance(self.requirements, list):
                raise FastmapException("Invalid 'requirements' format. It must be a "
                                       "list in 'package==1.2.3' form.")
            for req in self.requirements:
                if not REQUIREMENT_RE.match(req):
                    raise FastmapException("Invalid requirement format %r. Requirements "
                                           "must be formatted like 'package==1.2.3'." % req)
        self.log.restore_verbosity()  # undo hush

    @staticmethod
    def create(config=None, **kwargs):
        if not config and os.path.exists(DEFAULT_CONFIG_DIR):
            try:
                with open(DEFAULT_CONFIG_DIR) as f:
                    c = json.loads(f.read())
            except Exception as e:
                raise FastmapException(f"Exception loading '{DEFAULT_CONFIG_DIR}'") from e
        elif not config:
            c = dict(DEFAULT_INLINE_CONFIG)
        elif isinstance(config, dict):
            c = dict(config)
        elif isinstance(config, str):
            try:
                with open(config) as f:
                    c = json.loads(f.read())
            except Exception as e:
                raise FastmapException(f"Exception loading '{config}'") from e
        else:
            raise FastmapException(f"Unknown config type {type(config)}")

        for k, v in kwargs.items():
            if k not in DEFAULT_INLINE_CONFIG.keys():
                raise FastmapException(f"Unknown parameter: {k}")
            c[k] = v

        for k in DEFAULT_INLINE_CONFIG.keys():
            if k not in c:
                raise FastmapException(f"Missing configuration parameter: {k}")

        if c['machine_type'] not in MachineType:
            raise FastmapException(f"Unknown machine_type '{c['machine_type']}'.")

        if c['exec_policy'] not in ExecPolicy:
            raise FastmapException(f"Unknown exec_policy '{c['exec_policy']}'.")

        if c['exec_policy'] == ExecPolicy.LOCAL:
            # TODO
            # raise FastmapException("The LOCAL execution policy is not yet supported.")
            local_config = FastmapLocalConfig(c)
            atexit.register(local_exit_handler, local_config)
            return local_config
        return FastmapCloudConfig(c)

    @set_docstring(OFFLOAD_DOCSTRING)
    def offload(self, func: FunctionType, kwargs=None,
                hook=None, label=""):
        self.log.info("Fastmap offload." \
                      "\n  verbosity: %s." \
                      "\n  exec_policy: %s." % (self.verbosity, self.exec_policy))

        func_name = get_func_name(func)  # before applying kwargs, get func_name
        if kwargs:
            kwargs = kwargs or {}
            if not isinstance(kwargs, dict):
                raise FastmapException("'kwargs' must be a dict.")
            func = functools.partial(func, **kwargs)

        pickled_func = pickle_function(func, func_name)
        func_payload, func_hash = get_payload_and_hash(pickled_func, self)

        if self.exec_policy == ExecPolicy.LOCAL:
            task = FastmapLocalTask.create(self, func_payload, func_name=func_name,
                                           hook=hook, label=label)
            self.local_threads.append(task)
            return task

        assert self.exec_policy == ExecPolicy.CLOUD
        init_remote(self, func_hash, func_payload)
        return FastmapCloudTask.create(self, func_name=func_name, func_hash=func_hash,
                                       hook=hook, label=label)


class FastmapLocalConfig(FastmapConfig):
    pass


def check_task_id(func) -> FunctionType:
    @functools.wraps(func)
    def inner(self, task_id):
        if not task_id or not re.match(TASK_RE, task_id):
            raise FastmapException("Invalid task_id format %r" % task_id)
        return func(self, task_id)
    return inner


def make_dt(task):
    task['start_time'] = datetime.datetime.fromtimestamp(task['start_time'])


class FastmapCloudConfig(FastmapConfig):
    @check_task_id
    @set_docstring(POLL_DOCSTRING)
    def poll(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).poll()

    @set_docstring(POLL_ALL_DOCSTRING)
    def poll_all(self):
        try:
            resp = post_request(self.cloud_url + '/api/v1/poll_all', {},
                                self.secret, self.log)
        except CloudError as ex:
            # TODO
            raise ex
        tasks = resp.obj['tasks']
        list(map(make_dt, tasks))
        return tasks

    @check_task_id
    @set_docstring(RETRY_DOCSTRING)
    def retry(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).retry()

    @check_task_id
    @set_docstring(KILL_DOCSTRING)
    def kill(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).kill()

    @check_task_id
    @set_docstring(WAIT_DOCSTRING)
    def wait(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).wait()

    @check_task_id
    @set_docstring(RETURN_VALUE_DOCSTRING)
    def return_value(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).return_value()

    @check_task_id
    @set_docstring(TRACEBACK_DOCSTRING)
    def traceback(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).traceback()

    @check_task_id
    @set_docstring(CLEAR_DOCSTRING)
    def clear(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).clear()

    @set_docstring(CLEAR_ALL_DOCSTRING)
    def clear_all(self):
        try:
            resp = post_request(self.cloud_url + '/api/v1/clear_all', {},
                                self.secret, self.log)
        except CloudError as ex:
            # TODO
            raise ex
        self.log.info("Cleared %d tasks", resp.obj['count'])
        cleared_tasks = resp.obj['cleared_tasks']
        list(map(make_dt, cleared_tasks))
        return cleared_tasks

    @check_task_id
    @set_docstring(ALL_LOGS_DOCSTRING)
    def all_logs(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).all_logs()

    @check_task_id
    @set_docstring(NEW_LOGS_DOCSTRING)
    def new_logs(self, task_id):
        return FastmapCloudTask(self, task_id=task_id).new_logs()

    @set_docstring(MAP_DOCSTRING)
    def map(self, func: FunctionType, iterable: Iterable, kwargs=None,
            hook=None, label=""):
        raise AssertionError("This is not ready yet")

        if kwargs:
            kwargs = kwargs or {}
            if not isinstance(kwargs, dict):
                raise FastmapException("'kwargs' must be a dict.")
            func = functools.partial(func, **kwargs)

        pickled_func = pickle_function(func)
        func_payload, func_hash = get_payload_and_hash(pickled_func, self)

        if self.exec_policy == ExecPolicy.LOCAL:
            task = FastmapLocalTask.create_map(self, func_payload, func_name=func_name,
                                               iterable=iterable, hook=hook, label=label)
            self.local_threads.append(task)
            return task

        assert self.exec_policy == ExecPolicy.CLOUD
        init_remote(self, func_hash, func_payload)

        return FastmapCloudTask.create_map(self, func_hash=func_hash,
                                           iterable=iterable,
                                           hook=hook, label=label)


    # def _log_final_stats(self, fname: str, mapper: Mapper, proc_cnt: int,
    #                      total_dur: float):
    #     """ After finishing the .fastmap(...) run, log stats for the user """
    #     avg_runtime = mapper.avg_runtime
    #     total_credits_used = mapper.total_credits_used

    #     print()
    #     if not avg_runtime:
    #         self.log.info("Done processing %r in %.2fms." % (fname, total_dur*1000))
    #     else:
    #         time_saved = avg_runtime * proc_cnt - total_dur
    #         if time_saved > 0.02:
    #             self.log.info("Processed %d elements from %r in %s. "
    #                           "You saved ~%s.", proc_cnt, fname,
    #                           fmt_dur(total_dur), fmt_dur(time_saved))
    #         elif abs(time_saved) < 0.02:
    #             self.log.info("Processed %d elements from %r in %s. This "
    #                           "ran at about the same speed as the builtin map.",
    #                           proc_cnt, fname, fmt_dur(total_dur))
    #         elif self.exec_policy == ExecPolicy.LOCAL:
    #             self.log.info("Processed %d elements from %r in %s. This "
    #                           "ran slower than the map builtin by ~%s. "
    #                           "Consider not using fastmap here.",
    #                           proc_cnt, fname, fmt_dur(total_dur),
    #                           fmt_dur(time_saved * -1))
    #         else:
    #             self.log.info("Processed %d elements from %r in %s. "
    #                           "This ran slower than the map builtin by ~%s. "
    #                           "Consider connecting to a faster "
    #                           "internet, reducing your data size, or using "
    #                           "exec_policy LOCAL or ADAPTIVE.",
    #                           proc_cnt, fname, fmt_dur(total_dur),
    #                           fmt_dur(time_saved * -1))

    #     if total_credits_used:
    #         self.log.info("Spent $%.4f.", total_credits_used / 100)
    #     self.log.info("Fastmap done.")


def chunk_bytes(payload: bytes, size: int) -> list:
    return [payload[i:i + size] for i in range(0, len(payload), size)]


def init_remote(config, func_hash, func_payload):
    """
    Get the function and modules uploaded to the cloud via the
    /api/v1/init endpoint. This must happen BEFORE calling /api/v1/map.
    Because of server-side caching, and the potential for very large
    payloads, check with the function hash before uploading the function.
    """

    # Step 1: Try just uploaded the function hash. If it exists, we are good.
    req_dict = {}
    req_dict['func_hash'] = func_hash
    url = config.cloud_url + "/api/v1/init"
    try:
        resp = post_request(url, req_dict, config.secret, config.log)
    except CloudError as ex:
        # TODO
        raise ex

    if resp.status_code != 200:
        raise FastmapException("Cloud initialization failed %r." % resp.obj)
    if resp.status == InitStatus.FOUND:
        config.log.info("Function already on the server.")
        return
    if resp.status != InitStatus.NOT_FOUND:
        raise FastmapException("Unexpected init status %r." % resp.obj)

    # Step 2: If the server can't find the func, we need to upload it
    # We might need to chunk the upload due to cloud run limits
    func_parts = chunk_bytes(func_payload, 5 * MB)  # 5MB is arbitrary but feels right
    for i, func_part in enumerate(func_parts):
        req_dict['func'] = func_part
        req_dict['part_idx'] = i
        req_dict['part_len'] = len(func_parts)
        payload = msgpack.dumps(req_dict)
        payload_bytes = fmt_bytes(len(payload))
        if len(func_parts) > 1:
            config.log.info("Uploading code (%s) part %d/%d..." %
                            (payload_bytes, i + 1, len(func_parts)))
        else:
            config.log.info("Uploading code (%s)..." % payload_bytes)
        try:
            resp = post_request(url, payload, config.secret, config.log)
        except CloudError as ex:
            # TODO
            raise ex

        if resp.status_code != 200:
            raise FastmapException("Cloud initialization failed %r." % resp.obj)
        if resp.status == InitStatus.UPLOADED:
            continue
        raise FastmapException("Cloud initialization failed. Function not uploaded.")
    config.log.info("Done uploading code.")
    return


def get_payload_and_hash(pickled_func, config):
    local_sources, installed_mods, requirements = get_dependencies(
        config.requirements, config.log)
    func_payload = msgpack.dumps({
        'func': pickled_func,
        'local_sources': local_sources,
        'installed_mods': installed_mods,
        'requirements': requirements})
    compressed_payload = gzip.compress(func_payload, compresslevel=1)
    func_hash = get_hash(compressed_payload)
    return compressed_payload, func_hash


