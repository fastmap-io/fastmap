"""
Primary file for the fastmap SDK. Almost all client-side code is in this file.
Do not instantiate anything here directly. Use the interface __init__.py.
"""

import collections
import distutils.sysconfig
import glob
import gzip
import hashlib
import hmac
import importlib.metadata
import json
import multiprocessing
import os
import re
import secrets
import sys
import threading
import time
import traceback
from collections.abc import Iterable, Sequence, Generator
from types import FunctionType, ModuleType
from typing import List, Dict

import dill
import msgpack
import requests

SECRET_LEN = 64
SECRET_RE = r'^[0-9a-z]{64}$'
SITE_PACKAGES_RE = re.compile(r".*?/python[0-9.]+/(?:site|dist)\-packages/")
EXECUTION_ENV = "LOCAL"
CLIENT_VERSION = "0.0.6"
UNSUPPORTED_TYPE_STRS = ('numpy', 'pandas.DataFrame')
SUPPORTED_TYPES = (list, range, tuple, set, Generator)
KB = 1024
MB = 1024 ** 2
GB = 1024 ** 3

FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the results.
    Depending on prior configuration, fastmap will run either locally via
    multiprocessing, in the cloud on the fastmap.io servers, or adaptively on
    both.

    :param function func: Function to map against.
    :param sequence|generator iterable: Iterable to map over.
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

INIT_PARAMS = """
    :param str|file|dict config: The json file path / dict of the
        configuration. Every subsequent argument will alter this configuration.
        This is optional and if empty, only subsequent parameters will be used.
    :param str secret: The API token generated on fastmap.io. Treat this like a
        password. Do not commit it to version control! Failure to do so could
        result in man-in-the-middle attacks or your credits being used by others
        (e.g. cryptocurrency miners). If None, fastmap will run locally
        regardless of exec_policy.
    :param str verbosity: 'QUIET', 'NORMAL', or 'LOUD'. Default is 'NORMAL'.
    :param str exec_policy: 'LOCAL', 'CLOUD', or 'ADAPTIVE'.
        Default is 'ADAPTIVE'.
    :param int max_local_workers: How many local workers (processes) to start.
        By default, fastmap will start num_cores * 2 - 2.
    :param int max_cloud_workers: Maximum number of cloud workers to start.
        By default, fastmap will start 5.
    :param dict requirements: A dict of requirement->version like
        {'requests': '2.4.0'}. If omitted, requirement discovery is automatic.
    :param bool confirm_charges: Manually confirm cloud charges.

    For more documentation, go to https://fastmap.io/docs
    """

GLOBAL_INIT_DOCSTRING = """
    Initialize fastmap globally. All subsequent calls to fastmap will use this
    global configuration. Also see documentation for 'init'.
    %s
    :rtype: None

    Example usage:

        import fastmap

        fastmap.global_init(verbosity="LOUD")
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

        fastmap_config = fastmap.init(verbosity="LOUD")
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
ExecPolicy = Namespace("ADAPTIVE", "LOCAL", "CLOUD")
ReturnType = Namespace("ELEMENTS", "BATCHES")
Color = Namespace(
    GREEN="\033[92m",
    RED="\033[91m",
    YELLOW="\033[93m",
    CYAN="\033[36m",
    CANCEL="\033[0m")
AuthStatus = Namespace("AUTHORIZED")
InitStatus = Namespace("UPLOADED", "FOUND", "NOT_FOUND")
MapStatus = Namespace("NOT_FOUND", "BATCH_PROCESSED", "INITALIZING", "INITIALIZATION_ERROR", "PROCESS_ERROR")
DoneStatus = Namespace("DONE", "NOT_FOUND")

DEFAULT_CONFIG = {
    'secret': None,
    'cloud_url': None,
    'verbosity': Verbosity.NORMAL,
    'exec_policy': ExecPolicy.ADAPTIVE,
    'confirm_charges': False,
    'max_local_workers': None,
    'max_cloud_workers': 10,
    'requirements': None,
}


def set_docstring(docstr: str, docstr_prefix='') -> FunctionType:
    """ Add the given doc string to each function """
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


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


def get_func_hash(pickled_func: bytes) -> str:
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
    return hashlib.sha256(pickled_func).hexdigest()[:16]


class FastmapException(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """


class CloudError(Exception):
    """
    Thrown when the cloud connection results in a non-200

    The traceback (tb) can also be used to add more context to cloud errors
    """
    def __init__(self, *args, **kwargs):
        try:
            self.tb = kwargs.pop('tb')
        except KeyError:
            self.tb = None
        super().__init__(*args, **kwargs)


def simplified_tb(tb: str) -> str:
    """
    Given a traceback, remove fastmap-specific lines (this file + dependencies)
    to make it easier for the end user to read and hide the sausage-making.
    To do so, go through a traceback line-by-line in reverse. The moment
    we have a fastmap-specific line, break and return
    """
    jail_dir_paths = (
        '  File "' + os.path.abspath(__file__),
        '/layers/google.python.pip/pip/',
    )
    tb_list = []
    tb_lines = tb.split('\n')
    preamble = tb_lines.pop()  # we want "Traceback (most rec..." no matter what
    for tb_line in reversed(tb_lines):
        if any(tb_line.startswith(path) for path in jail_dir_paths):
            # pop prev tb_line b/c each stack layer is 2 lines: file-loc & code
            tb_list.pop()
            break
        tb_list.append(tb_line)
    tb_list.append(preamble)
    return '\n'.join(reversed(tb_list)).strip()


def func_name(func: FunctionType) -> str:
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
        return input(Color.CYAN + "\n fastmap: " + msg + Color.CANCEL)


class InterThreadDataManager():
    """
    Does task allocation between various threads both local and remote.
    Importantly, this has an inbox and an outbox.
    The various local and remote workers will pull batches out of the inbox.
    Once done, those batches go to the outbox.
    A lot of the apparent complexity is to deal with multiprocessing issues.
    """
    def __init__(self, first_runtime: float, max_batches_in_queue: int):
        manager = multiprocessing.Manager()
        self._lock = multiprocessing.Lock()
        self._inbox = manager.list()
        self._outbox = manager.list()
        self._runtimes = manager.list()
        self._errors = multiprocessing.Queue()

        self._runtimes.append(first_runtime)

        self.state = manager.dict()
        self.state['inbox_capped'] = False
        self.state['inbox_tot'] = 0
        self.state['outbox_tot'] = 0
        self.state['total_credits_used'] = 0
        self.state['total_network_seconds'] = 0.0

    def inbox_len(self):
        """
        How many batches still in the inbox?
        This is convoluted because we are dealing with inter-process queues
        """
        if not self.state['inbox_capped']:
            return sys.maxsize
        return self.state['inbox_tot'] - self.state['outbox_tot']

    def has_more_batches(self):
        """
        Are there more batches in the inbox or outbox?
        Used to keep waiting for the outbox
        """
        while True:
            if self.state['inbox_tot'] or self.state['inbox_capped']:
                break
            time.sleep(.01)
        if self.inbox_len() > 0:
            return True
        return len(self._outbox) > 0

    def get_total_credits_used(self):
        return self.state['total_credits_used']

    def get_total_network_seconds(self):
        return self.state['total_network_seconds']

    def get_error(self):
        return self._errors.get_nowait()

    def put_error(self, error_origin: str, error_str: str, batch_tup=None):
        """
        Manage the error and put problem batch onto the front of the inbox
        so we can try again.
        """
        self._errors.put((error_origin, error_str))
        if batch_tup:
            with self._lock:
                self._inbox.insert(0, batch_tup)

    def kill(self):
        """
        If a worker throws an error somewhere, this will be called so that
        we can gracefully exit
        """
        with self._lock:
            self.state['inbox_capped'] = True
            self._inbox = None
            self._outbox = None
            self._runtimes = None

    def mark_inbox_capped(self):
        """
        Once the _FillInbox thread completes, a capped inbox lets us know
        that we can kill everything once inbox_tot - outbox_tot == 0
        """
        with self._lock:
            assert not self.state['inbox_capped']
            self.state['inbox_capped'] = True
            self.state['inbox_started'] = True

    def push_inbox(self, batch: list):
        """
        Called from one of the _FillInbox subclasses.
        Add to the inbox and maintain state
        """
        with self._lock:
            self._inbox.append(batch)
            self.state['inbox_tot'] += 1

    def total_avg_runtime(self):
        return sum(self._runtimes) / len(self._runtimes)

    def checkout(self):
        """
        Repeatedly try popping a batch off the inbox.
        If not capped, keep trying.
        """
        while True:
            if self.state['inbox_capped']:
                # If capped, assume that if we don't get a batch from the inbox
                # that means we are exhausted and send the poison pill
                with self._lock:
                    try:
                        return self._inbox.pop(0)
                    except IndexError:
                        return None
                    except OSError:
                        print("Too many threads. Shutting one down...")
                        sys.exit(0)

            with self._lock:
                try:
                    return self._inbox.pop(0)
                except IndexError:
                    pass
                except OSError:
                    print("Too many threads. Shutting one down...")
                    sys.exit(0)
            time.sleep(.01)

    def push_outbox(self, batch_idx: int, processed_batch: list, runtime: float,
                    credits_used=None, network_seconds=None):
        """
        Once processed, push the batch onto the outbox and manage other state
        """
        with self._lock:
            self._outbox.append((batch_idx, processed_batch))
            self.state['outbox_tot'] += 1
            if runtime:
                self._runtimes.append(runtime)
            if credits_used:
                self.state['total_credits_used'] += credits_used
            if network_seconds:
                self.state['total_network_seconds'] += network_seconds

    def pop_outbox(self, workers: list):
        """
        Called repeatedly by a thread which waits for workers to finish
        Also checks for whether any worker is alive
        """
        while True:
            with self._lock:
                if len(self._outbox):
                    return self._outbox.pop(0)
            if not any(w.is_alive() for w in workers):
                ex_str = "While trying to get next processed element"
                raise FastmapException(ex_str)
            time.sleep(.01)


def local_worker_func(func: FunctionType, itdm: InterThreadDataManager,
                      log: FastmapLogger) -> None:
    """
    A single persistent local worker. This function will process one
    batch at a time until there are none left.
    """
    func = dill.loads(func)
    try:
        batch_tup = itdm.checkout()
        while batch_tup:
            batch_idx, batch_iter = batch_tup
            start = time.perf_counter()
            ret = list(map(func, batch_iter))
            total_proc_time = time.perf_counter() - start
            runtime = total_proc_time / len(ret)
            log.debug("Batch %d local cnt=%d dur=%.2fs (%.2e/el).",
                      batch_idx, len(ret), total_proc_time, runtime)
            itdm.push_outbox(batch_idx, ret, runtime)
            batch_tup = itdm.checkout()
    except Exception as e:
        proc_name = multiprocessing.current_process().name
        itdm.put_error(proc_name, repr(e), batch_tup)
        tb = simplified_tb(traceback.format_exc())
        log.error("In local worker [%s]:\n %s.",
                  multiprocessing.current_process().name, tb)
        return


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
    }


def post_request(url: str, data: bytes, secret: str,
                 log: FastmapLogger) -> requests.Response:
    """
    Generic cloud post wrapper.
    This does warning/error management, extracts content, and checks signatures
    for every API post
    """
    if isinstance(data, dict):
        data = msgpack.dumps(data)

    headers = basic_headers(secret, data)
    try:
        resp = requests.post(url, data=data, headers=headers)
    except requests.exceptions.ConnectionError:
        raise CloudError("Fastmap could not connect to %r. "
                         "Check your connection." % url) from None
    if 'X-Server-Warning' in resp.headers:
        # deprecations or anything else
        log.warning(resp.headers['X-Server-Warning'])

    if resp.status_code == 500:
        raise CloudError("Fastmap cloud error: %r" % resp.content)

    if resp.headers.get('Content-Type') == 'application/msgpack':
        resp.obj = msgpack.loads(resp.content)
        resp.status = resp.headers['X-Status']
    elif resp.headers.get('Content-Type') == 'application/octet-stream':
        if 'X-Content-Signature' not in resp.headers:
            raise CloudError("Cloud payload was not signed (%d). "
                             "Will not unpickle." % (resp.status_code))
        cloud_hash = hmac_digest(secret, resp.content)
        if resp.headers['X-Content-Signature'] != cloud_hash:
            raise CloudError("Cloud checksum did not match. Will not unpickle.")
        resp.status = resp.headers['X-Status']
        try:
            resp.obj = dill.loads(gzip.decompress(resp.content))
        except gzip.BadGzipFile:
            raise CloudError("Error unzipping response") from None
        except dill.UnpicklingError:
            raise CloudError("Error unpickling response") from None
    else:
        raise CloudError("Unexpected Content-Type %r (%d): %r" %
                         (resp.headers.get("Content-Type"),
                          resp.status_code,
                          resp.content[:100].strip()))

    return resp


def process_cloud_batch(itdm: InterThreadDataManager, batch_tup: tuple,
                        map_url: str, func_hash: str, label: str,
                        run_id: str, secret: str, log: FastmapLogger) -> None:
    """
    For /api/v1/map, finish preparing the request, send it, and handle the
    response. Processed batches go back into the itdm. If a
    processed batch leaves this function, it will end up back with the user.
    """

    start_req_time = time.perf_counter()

    batch_idx, batch = batch_tup
    try:
        pickled_batch = dill.dumps(batch)
    except Exception as ex:
        raise CloudError("Could not pickle your data. "
                         "Fastmap cannot run on the cloud.") from ex
    compressed_batch = gzip.compress(pickled_batch, compresslevel=1)
    req_dict = {
        'func_hash': func_hash,
        'batch': compressed_batch,
        'label': label,
        'run_id': run_id,
    }
    payload = msgpack.dumps(req_dict)

    while True:
        log.debug("Making cloud request batchlen=%d size=%s (%s/el)...",
                  len(batch), fmt_bytes(len(compressed_batch)),
                  fmt_bytes(len(compressed_batch) / len(batch)))
        resp = post_request(map_url, payload, secret, log)
        if resp.status_code == 200:
            if resp.status == MapStatus.INITALIZING:
                log.debug("Cloud worker is initializing. Last msg [%s]."
                          " Retrying in 5 seconds..." %
                          (resp.obj.get('init_step', '')))
                time.sleep(5)
                continue
            elif resp.status == MapStatus.INITIALIZATION_ERROR:
                raise CloudError("Error initializing worker %r %r" % (
                                 resp.obj.get('init_error'), resp.obj.get('init_tb')))
        break

    mem_used = (float(resp.headers.get('X-Mem-Used', 0.0))
                / float(resp.headers.get('X-Mem-Total', 1.0)))
    if mem_used > 0.9:
        log.warning("Cloud memory utilization high: %.2f%%. "
                    "Consider increasing memory." % mem_used * 100)

    if resp.status == MapStatus.BATCH_PROCESSED:
        service_id = resp.headers['X-Service-Id']
        total_request = time.perf_counter() - start_req_time
        total_application = float(resp.headers['X-Application-Seconds'])
        total_mapping = float(resp.headers['X-Map-Seconds'])
        credits_used = float(resp.headers['X-Credits'])
        result_len = len(resp.obj['results'])
        req_time_per_el = total_request / result_len
        app_time_per_el = total_application / result_len
        map_time_per_el = total_mapping / result_len

        log.debug("Batch %d cloud cnt=%d "
                  "%.2fs/%.2fs/%.2fs map/app/req (%.2e/%.2e/%.2e per el) "
                  "[%s].",
                  batch_idx, result_len,
                  total_mapping, total_application, total_request,
                  map_time_per_el, app_time_per_el, req_time_per_el,
                  service_id)
        itdm.push_outbox(batch_idx,
                         resp.obj['results'],
                         None,
                         credits_used=credits_used,
                         network_seconds=total_request - total_application)
        return

    if resp.status == MapStatus.PROCESS_ERROR:
        # ERROR
        msg = "Your code could not be processed on the cloud: %s. " % \
            resp.obj.get('exception')
        bad_modules = resp.obj.get('bad_modules', [])
        if bad_modules:
            msg += "Modules with errors on import: %s." % ' '.join(bad_modules)
            msg += "You might need to explicitly specify a requirements file " \
                   "in your deployment."
        raise CloudError(msg, tb=resp.obj.get('traceback', ''))
    if resp.status == MapStatus.NOT_FOUND:
        msg = "Your function was not found on the cloud."
        raise CloudError(msg)
    if resp.status_code == 401:
        # UNAUTHORIZED
        raise CloudError("Unauthorized. Check your API token.")
    if resp.status_code == 402:
        # NOT_ENOUGH_CREDITS
        raise CloudError("Insufficient credits for this request. "
                         "Your current balance is $%.4f." %
                         resp.obj.get('credits_used', 0) / 100)
    if resp.status_code == 403:
        # INVALID_SIGNATURE
        raise CloudError("Invalid signature. Check your token")
    if resp.status_code == 410:
        # DISCONTINUED (post-deprecated end-of-life)
        raise CloudError("Fastmap.io API discontinued: %r" % resp.obj.get('reason'))
    if resp.status_code == 413:
        # TOO_LARGE
        raise CloudError("Your request was too large (%s). "
                         "Find a way to reduce the size of your data or "
                         "function and try again." % fmt_bytes(len(payload)))

    if resp.status_code == 500 and resp.headers['Content-Type'] == 'text/html':
        content = re.sub('<[^<]+?>', '', resp.text)
        raise CloudError("Unexpected cloud error 500. You might have run out "
                         "of memory. %s" % content)

    # catch all (should just be for 500s of which a few are explicitly defined)
    raise CloudError("Unexpected cloud response %d %s %r" %
                     (resp.status_code, resp.status, resp.obj))


def cloud_thread(thread_id: str, map_url: str, func_hash: str, label: str,
                 run_id: str, itdm: InterThreadDataManager, secret: str,
                 log: FastmapLogger):
    """
    A thread for running cloud requests in a loop. Batches are pulled out of
    the itdm and passed into process_cloud_batch one-by-one until they are
    exhausted. This also does some basic
    """
    batch_tup = itdm.checkout()
    if batch_tup:
        log.debug("Starting cloud thread %d...", thread_id)
    while batch_tup:
        try:
            process_cloud_batch(itdm, batch_tup, map_url, func_hash,
                                label, run_id, secret, log)
        except CloudError as e:
            proc_name = multiprocessing.current_process().name
            thread_id = threading.get_ident()
            error_loc = "%s: thread:%d" % (proc_name, thread_id)
            itdm.put_error(error_loc, repr(e), batch_tup)
            if hasattr(e, 'tb') and e.tb:
                tb = e.tb.replace('%0A', '\n')
                log.error("In cloud thread [%s]:\n%s.",
                          threading.current_thread().name, tb)
            else:
                log.error("In cloud thread [%s]: %r.",
                          threading.current_thread().name, e)
            log.error("Shutting down cloud thread due to error...")
            return

        batch_tup = itdm.checkout()


def get_modules(log: FastmapLogger) -> (Dict[str, str], List[ModuleType]):
    """
    Get in scope modules.
    Returns two things:
    1.  a dictionary of all mod_name -> source|None
    2.  a list of ModuleType for modules found in site packages
    For the former, a source is included if it is a local module. If it is
    an installed module, the source is None
    """
    std_lib_dir = distutils.sysconfig.get_python_lib(standard_lib=True)
    local_sources = {}
    installed_mods = []
    for mod_name, mod in sys.modules.items():
        if (mod_name in sys.builtin_module_names or  # not builtin # noqa
            mod_name.startswith("_") or  # not hidden # noqa
            not getattr(mod, '__file__', None) or   # also not builtin # noqa
            mod.__file__.startswith(std_lib_dir) or  # not stdlib # noqa
            (hasattr(mod, "__package__") and  # noqa
             mod.__package__ in ("fastmap", "fastmap.fastmap"))):  # not fastmap
                continue  # noqa
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
    Get pip installed requirements which are loaded into the
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
            # print('\a'); import ipdb; ipdb.set_trace()
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

    # print('\a'); import ipdb; ipdb.set_trace()

    return requirements


def get_dependencies(requirements: dict, log: FastmapLogger) -> (dict, dict):
    """
    Get dependency dictionary.
    Keys are module names.
    Values are either pip version strings or source code.
    """
    local_sources, installed_mods = get_modules(log)
    requirements = requirements or get_requirements(installed_mods, log)
    installed_mods = [im.__name__ for im in installed_mods]

    # TODO
    # req_str = ' '.join(k + "==" + v for k, v in sorted(requirements.items()))
    # mods_str = ', '.join(sorted(local_sources.keys()))
    # log.debug("Found %d installed modules" % len(installed_mods))
    # log.debug("Found requirements [%s]" % req_str)
    # log.debug("Found local imports [%s]" % mods_str)

    return local_sources, installed_mods, requirements


def post_done(cloud_url: str, secret: str, log: FastmapLogger,
              func_hash: str, run_id: str) -> None:
    """
    Once fastmap has finished, post done to suggest that the server tidy up.
    If this is not called, the server will do it anyway on a cron schedule.
    """

    url = cloud_url + '/api/v1/done'
    payload = msgpack.dumps({
        "func_hash": func_hash,
        "run_id": run_id
    })
    resp = post_request(url, payload, secret, log)

    if resp.status_code != 200 or resp.status != DoneStatus.DONE:
        log.error("Error on final api call (%d) %r.",
                  resp.status_code, resp.status)


class _FillInbox(threading.Thread):
    """
    Base class of the two inbox fillers which both fill up the itdm inbox with
    the iterable.
    """
    BATCH_DUR_GOAL = .2

    def __init__(self, iterable: Iterable, itdm: InterThreadDataManager,
                 log: FastmapLogger, avg_runtime: float, *args, **kwargs):
        self.iterable = iterable
        self.itdm = itdm
        self.log = log
        self.avg_runtime = avg_runtime
        self.cnt = 0
        self.do_die = False
        self.capped = False
        super().__init__(*args, **kwargs)

    def kill(self):
        """ This exists in case an error is thrown in a worker process """
        self.do_die = True

    def batch_len(self) -> int:
        """ Return a length that gets us close to BATCH_DUR_GOAL seconds """
        return max(1, int(self.BATCH_DUR_GOAL / self.avg_runtime))


def gen_batcher(generator: Generator, size: int) -> Generator:
    """
    Batch a generator into iterables of size

    We might be able to do this faster.
    Originally, I was using a recipe adapted from here:
    https://stackoverflow.com/questions/8290397/
    how-to-split-an-iterable-in-constant-size-chunks
    However, that failed when we have falsy raw values
    in the generator (e.g. 0, None, False, etc.)
    I DO have a test for that now: test_reversed_range
    """
    batch = []
    try:
        while True:
            while len(batch) < size:
                batch.append(next(generator))
            yield batch
            batch = []
    except StopIteration:
        if batch:
            yield batch


class FillInboxWithGen(_FillInbox):
    """
    For generators, this fills the ITDM inbox in a thread
    """
    def run(self):
        batch_cnt = 0
        element_cnt = 0

        for batch in gen_batcher(self.iterable, self.batch_len()):
            self.itdm.push_inbox((batch_cnt, batch))
            batch_cnt += 1
            element_cnt += len(batch)
            if self.do_die:
                return
        self.log.debug("Done adding iterator to task inbox. %d batch(es). "
                       "%d element(s) total.", batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped()


class FillInboxWithSeq(_FillInbox):
    """
    For sequences, this fills the ITDM inbox in a thread
    """
    def run(self):
        batch_len = self.batch_len()

        seq_len = len(self.iterable)
        batch_cnt = 0
        element_cnt = 0
        for idx in range(0, seq_len, batch_len):
            batch = self.iterable[idx:min(idx + batch_len, seq_len)]
            self.itdm.push_inbox((batch_cnt, batch))
            batch_cnt += 1
            element_cnt += len(batch)
            if self.do_die:
                return
        self.log.debug("Done adding sequence to task inbox. %d batch(es). "
                       "%d element(s) total.", batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped()


def seq_progress(seq: Sequence, log: FastmapLogger, seq_len: int,
                 start_time: float) -> Generator:
    """
    Progress printer and counter for sequences
    """
    proc_cnt = 0
    percenters = list(range(10, 100, 10))
    for batch in seq:
        proc_cnt += len(batch)
        yield batch, proc_cnt
        percent = proc_cnt / seq_len * 100
        elapsed_time = time.perf_counter() - start_time
        num_left = seq_len - proc_cnt
        time_remaining = elapsed_time * num_left / proc_cnt
        progress = "%sfastmap: %.1f%% (%s remaining)%s\r" % (
            Color.GREEN, percent, fmt_time(time_remaining), Color.CANCEL)
        sys.stdout.write(progress)
        sys.stdout.flush()
        if not percenters or percent < percenters[0]:
            continue
        while percenters and percent >= percenters[0]:
            percenters = percenters[1:]
        print(progress[:-1])


def gen_progress(gen: Generator, log: FastmapLogger, *args) -> Generator:
    """
    Progress printer for generators where the length of the generator is
    not known in advance.
    Extra args are meant for seq_progress and are thus discarded
    """
    proc_cnt = 0
    for batch in gen:
        proc_cnt += len(batch)
        yield batch, proc_cnt
        progress = "%sfastmap: Processed %d%s\r" % (
            Color.GREEN, proc_cnt, Color.CANCEL)
        sys.stdout.write(progress)
        sys.stdout.flush()


class AuthCheck(threading.Thread):
    """
    Thread to check the authorization status. This allows us to kill the
    process before packaging and sending up a potentially large payload
    """
    def __init__(self, config, *args, **kwargs):
        self.secret = config.secret
        self.url = config.cloud_url + '/api/v1/auth'
        self.log = config.log
        super().__init__(*args, **kwargs)

    def run(self):
        payload = msgpack.dumps({})
        try:
            resp = post_request(self.url, payload, self.secret, self.log)
        except CloudError as ex:
            self.log.error("During auth check [%s]:\n%s.",
                           multiprocessing.current_process().name, ex)
            self.success = False
            return
        if resp.status_code == 200 and resp.status == AuthStatus.AUTHORIZED:
            self.log.info("Request to %r was successful.", self.url)
        else:
            self.log.warning("Authentication failed for %r %r. Check your token.",
                             self.url, resp.status)
            self.success = False
        self.success = True

    def was_success(self):
        # exists for mocks
        return self.success


class Mapper():
    """
    Wrapper for running fastmap.
    Each call to FastmapConfig.fastmap(...) generates one of these.
    Stores execution state specifically and is thus separate from FastmapConfig
    """
    INITIAL_RUN_DUR = 0.1  # seconds
    PROC_OVERHEAD = 0.1  # seconds

    def __init__(self, config):
        self.config = config
        self.log = config.log
        self.avg_runtime = None
        self.avg_egress = None
        self.total_credits_used = 0
        self.total_network_seconds = 0

        self.workers = []
        self.fill_inbox_thread = None
        self.itdm = None

        if self.config.exec_policy != ExecPolicy.LOCAL:
            self.auth_check = AuthCheck(self.config)
            self.auth_check.start()
        else:
            self.auth_check = None

    def cleanup(self):
        """
        Live processes, threads, and pipes cause scripts to hang.
        This cleans them all out in the case of an error
        """
        self.log.info("Cleaning up threads and processes because of error...")
        for p in self.workers:
            p.join()
        if self.fill_inbox_thread:
            try:
                self.fill_inbox_thread.kill()
            finally:
                self.fill_inbox_thread.join()
        if self.itdm:
            self.itdm.kill()
        self.log.info("Threads and processes clean.")

    def _confirm_charges(self, iterable: Iterable) -> bool:
        """
        In a loop, present a user dialog to confirm estimated charges.
        Works with both generator and sequence types
        """
        if not self.config.confirm_charges:
            return True
        while True:
            if hasattr(iterable, "__len__"):
                credit_estimate = get_credits(self.avg_runtime * len(iterable),
                                              self.avg_egress * len(iterable))
                user_input_query = "Estimate: $%.4f. Continue?" % \
                    (credit_estimate / 100)
            else:
                user_input_query = "Cannot estimate credit usage because " \
                                   "iterable is a generator. Continue anyway?"
            user_input = self.log.input("%s (y/n) " % user_input_query)
            if user_input.lower() == 'y':
                return True
            if user_input.lower() == 'n':
                if self.config.exec_policy == ExecPolicy.ADAPTIVE:
                    self.log.info("Cloud operation cancelled. "
                                  "Continuing processing locally...")
                return False
            self.log.warning("Unrecognized input of %r. "
                             "Please input 'y' or 'n'.", user_input)

    def _get_workers(self, func: FunctionType, iterable: Iterable,
                     label: str) -> (list, InterThreadDataManager):
        """
        Fastmap works through a list of workers pulling batches from the
        inbox of the InterThreadDataManager (itdm) and pushing batches onto
        the outbox of the itdm. This returns both the workers and the itdm.

        """
        # TODO make this smarter
        if self.config.exec_policy == ExecPolicy.CLOUD:
            max_batches_in_queue = self.config.max_cloud_workers
        elif self.config.exec_policy == ExecPolicy.LOCAL:
            max_batches_in_queue = self.config.max_local_workers
        else:
            max_batches_in_queue = self.config.max_local_workers + \
                                   self.config.max_cloud_workers  # noqa

        try:
            itdm = InterThreadDataManager(self.avg_runtime, max_batches_in_queue)
        except EOFError as ex:
            self.log.error("Error starting multiprocessing. To fix, you "
                           "probably just need to run your code in the main "
                           "context. E.g:\n"
                           "if __name__ == '__main__':\n"
                           "    # start running program here")
            raise FastmapException(
                "Error starting multiprocessing. See the "
                f'"{Color.RED}fastmap ERROR{Color.CANCEL}" above.') from ex

        try:
            pickled_func = dill.dumps(func, recurse=True)
        except Exception as ex:
            func_name = getattr(func, "__name__", str(func))
            err = "Your function %r could not be pickled." % func_name
            raise FastmapException(err) from ex

        workers = []
        if self.config.exec_policy != ExecPolicy.CLOUD:
            for _ in range(self.config.max_local_workers):
                proc_args = (pickled_func, itdm, self.log)
                local_worker = multiprocessing.Process(target=local_worker_func,
                                                       args=proc_args)
                local_worker.start()
                workers.append(local_worker)
            self.log.debug("Launching %d local workers...",
                           self.config.max_local_workers)
        if self.config.exec_policy != ExecPolicy.LOCAL:
            self.auth_check.join()
            if self.auth_check.was_success() and self._confirm_charges(iterable):
                cloud_supervisor = _CloudSupervisor(pickled_func, itdm,
                                                    self.config, label)
                cloud_supervisor.start()
                workers.append(cloud_supervisor)

        if not workers or not any(p.is_alive() for p in workers):
            raise FastmapException("No execution workers started. "
                                   "This was likely triggered by another error above.")

        return workers, itdm

    def initial_run(self, func: FunctionType,
                    iterable: Iterable) -> (list, bool):
        """
        Maps the function over the iterable for a short period of time to
        estimate the avg runtime which will is used to make decisions about
        the best way to execute the map (via multiprocessing, remotely,
        batch_size, etc.)
        :returns: the processed results, possibly more (but not certainly)
        """
        self.log.debug("Estimating runtime with an initial test run...")
        iterable = iter(iterable)
        ret = []
        start_time = time.perf_counter()
        end_loop_at = start_time + self.INITIAL_RUN_DUR
        while True:
            try:
                item = next(iterable)
            except StopIteration:
                if not ret:
                    # Zero item case
                    return [], False
                self.avg_runtime = (time.perf_counter() - start_time) / len(ret)
                self.log.debug("Initial test run processed entire iterable.")
                return ret, False
            ret.append(func(item))
            if time.perf_counter() > end_loop_at:
                break
        first_batch_run_time = time.perf_counter() - start_time
        len_first_batch = len(ret)
        try:
            size_egress = len(dill.dumps(ret))
        except Exception as ex:
            raise FastmapException("Could not pickle your results.") from ex
        self.avg_runtime = first_batch_run_time / len_first_batch
        self.avg_egress = size_egress / len_first_batch
        self.log.debug("Processed first %d elements in %.2fs (%.2e/element).",
                       len_first_batch, first_batch_run_time, self.avg_runtime)
        return ret, True

    def _estimate_multiproc_runtime(self, iterable: Iterable) -> float:
        """
        Provides a best-guess estimate of how long it would take to run
        on the available workers
        """
        local_runtime = self.avg_runtime * len(iterable)
        total_proc_overhead = self.config.max_local_workers * self.PROC_OVERHEAD
        total_process_runtime = local_runtime / self.config.max_local_workers
        return total_proc_overhead + total_process_runtime

    def map(self, func: FunctionType, iterable: Iterable, label: str,
            is_seq=False) -> Generator:
        """
        Contains most of the business logic for how and where the iterable
        will be processed. This also creates the itdm and starts it running.
        """

        if is_seq:
            if not hasattr(iterable, '__getitem__'):
                # We need indexing capability. Iterable likely a set
                iterable = list(iterable)
            seq_len = len(iterable)
            self.log.info("Applying %r to %d items and yielding the results...",
                          func_name(func), seq_len)
        else:
            self.log.info("Applying %r to a generator and yielding the "
                          "results...", func_name(func))

        initial_batch, maybe_more = self.initial_run(func, iterable)
        yield initial_batch
        if not maybe_more:
            return

        if is_seq:
            # remove already processed batch from iterable
            iterable = iterable[len(initial_batch):]

        if is_seq and self.config.exec_policy != ExecPolicy.CLOUD:
            # If initial run is especially fast, run single-threaded local
            local_runtime = self.avg_runtime * len(iterable)
            if local_runtime < self._estimate_multiproc_runtime(iterable):
                self.log.debug("Running single-threaded due to "
                               "short expected runtime.")
                yield list(map(func, iterable))
                return

        if self.config.max_local_workers <= 1 and \
           self.config.exec_policy == ExecPolicy.LOCAL:
            # If local, and only one process is available, we have no choice
            # except to run single-threaded
            self.log.debug("Running single-threaded due to having <= 1 "
                           "process available.")
            yield list(map(func, iterable))
            return

        self.workers, self.itdm = self._get_workers(func, iterable, label)

        if is_seq:
            self.fill_inbox_thread = FillInboxWithSeq(
                iterable, self.itdm, self.log, self.avg_runtime)
        else:
            self.fill_inbox_thread = FillInboxWithGen(
                iterable, self.itdm, self.log, self.avg_runtime)
        self.fill_inbox_thread.start()

        cur_idx = 0
        staging = {}
        while self.itdm.has_more_batches():
            result_idx, result_list = self.itdm.pop_outbox(self.workers)
            staging[result_idx] = result_list
            while cur_idx in staging:
                yield staging.pop(cur_idx)
                cur_idx += 1

        self.fill_inbox_thread.join()
        for worker in self.workers:
            worker.join()

        self.total_credits_used = self.itdm.get_total_credits_used()
        self.avg_runtime = self.itdm.total_avg_runtime()
        self.total_network_seconds = self.itdm.get_total_network_seconds()


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
        "confirm_charges",
        "max_local_workers",
        "max_cloud_workers",
        "cloud_url",
        "requirements",
    ]

    def __init__(self, config=None, **kwargs):
        if not config:
            c = dict(DEFAULT_CONFIG)
        elif isinstance(config, dict):
            c = dict(config)
        elif isinstance(config, str):
            with open(config) as f:
                c = json.loads(f.read())
        else:
            raise FastmapException(f"Unknown config type {type(config)}")

        for k, v in kwargs.items():
            if k not in DEFAULT_CONFIG.keys():
                raise FastmapException(f"Unknown parameter: {k}")
            c[k] = v

        if c['exec_policy'] not in ExecPolicy:
            raise FastmapException(f"Unknown exec_policy '{c['exec_policy']}'.")
        self.exec_policy = c['exec_policy']
        self.log = FastmapLogger(c['verbosity'])
        self.verbosity = c['verbosity']
        self.cloud_url = c['cloud_url']
        self.confirm_charges = c['confirm_charges']
        self.max_cloud_workers = c['max_cloud_workers']
        self.requirements = c['requirements']

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

        if c['secret']:
            if not isinstance(c['secret'], str) or not re.match(SECRET_RE, c['secret']):
                raise FastmapException("Invalid secret token.")
            self.secret = c['secret']
        else:
            self.secret = None
            if self.exec_policy != ExecPolicy.LOCAL:
                self.exec_policy = ExecPolicy.LOCAL
                self.log.warning("No secret provided. "
                                 "Setting exec_policy to LOCAL.")

        if self.requirements is not None and not isinstance(self.requirements, dict):
            raise FastmapException("Invalid dependencies format.")

        if c['max_local_workers']:
            self.max_local_workers = c['max_local_workers']
        else:
            self.max_local_workers = os.cpu_count() - 2

        if self.max_cloud_workers > 100:
            self.log.warning("More than 100 cloud workers will likely cause "
                             "the fastmap server to crash. You have %d",
                             self.max_cloud_workers)

        if not self.confirm_charges and self.exec_policy != ExecPolicy.LOCAL:
            pass
            # TODO
            # self.log.warning("You are set to automatically use cloud "
            #                  "resources without confirmation. To get "
            #                  "estimates before usage, set confirm_charges=True.")

        self.log.info("Setup fastmap.")
        self.log.info(" verbosity: %s.", self.verbosity)
        self.log.info(" exec_policy: %s.", self.exec_policy)
        if self.exec_policy != ExecPolicy.CLOUD:
            self.log.info(" max_local_workers: %d.", self.max_local_workers)
        if self.exec_policy != ExecPolicy.LOCAL:
            self.log.info(" max_cloud_workers: %d.", self.max_cloud_workers)
        self.log.restore_verbosity()  # undo hush

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func: FunctionType, iterable: Iterable,
                return_type=ReturnType.ELEMENTS, label="") -> Generator:
        """
        Entry for fastmap. This function handles the numerous different
        ways to print and return processed batches. This is also the final
        handler for errors and exception
        """
        if return_type not in ReturnType:
            raise FastmapException(f"Unknown return_type '{return_type}'")
        iter_type = str(type(iterable))
        if any(t in iter_type for t in UNSUPPORTED_TYPE_STRS):
            self.log.warning(f"Iterable type '{iter_type}' is explicitly not supported.")
        elif not any(isinstance(iterable, t) for t in SUPPORTED_TYPES):
            self.log.warning(f"Iterable type '{iter_type}' is not explictly supported.")

        start_time = time.perf_counter()
        is_seq = hasattr(iterable, '__len__')
        seq_len = None

        if is_seq:
            seq_len = len(iterable)
            if not seq_len:
                return

        mapper = Mapper(self)
        fm = mapper.map(func, iterable, label, is_seq=is_seq)
        errors = []

        try:
            if self.verbosity in (Verbosity.SILENT, Verbosity.QUIET):
                if return_type == ReturnType.BATCHES:
                    for batch in fm:
                        yield batch
                else:
                    # else return_type is ELEMENTS
                    for batch in fm:
                        for el in batch:
                            yield el
                return

            proc_cnt = 0
            progress_func = seq_progress if is_seq else gen_progress

            if return_type == ReturnType.BATCHES:
                for batch, proc_cnt in progress_func(fm, self.log, seq_len,
                                                     start_time):
                    yield batch
            else:
                # else return_type is ELEMENTS
                for batch, proc_cnt in progress_func(fm, self.log, seq_len,
                                                     start_time):
                    for el in batch:
                        yield el

        except Exception as e:
            if mapper.itdm:
                try:
                    while True:
                        worker_error = mapper.itdm.get_error()
                        errors.append(" Worker error [%s]: %s" % (
                                      worker_error[0], worker_error[1]))
                except multiprocessing.queues.Empty:
                    pass
            mapper.cleanup()

            if not errors or not isinstance(e, FastmapException):
                raise e from None

        if errors:
            error_msg = "Every execution worker died. List of errors:"
            for error in errors:
                error_msg += "\n  " + error
            self.log.error(error_msg)
            raise FastmapException(error_msg)

        total_dur = time.perf_counter() - start_time
        self._log_final_stats(func_name(func), mapper, proc_cnt,
                              total_dur)

    def _log_final_stats(self, fname: str, mapper: Mapper, proc_cnt: int,
                         total_dur: float):
        """ After finishing the .fastmap(...) run, log stats for the user """
        avg_runtime = mapper.avg_runtime
        total_credits_used = mapper.total_credits_used

        print()
        if not avg_runtime:
            self.log.info("Done processing %r in %.2fms." % (fname, total_dur*1000))
        else:
            time_saved = avg_runtime * proc_cnt - total_dur
            if time_saved > 0.02:
                self.log.info("Processed %d elements from %r in %s. "
                              "You saved ~%s.", proc_cnt, fname,
                              fmt_dur(total_dur), fmt_dur(time_saved))
            elif abs(time_saved) < 0.02:
                self.log.info("Processed %d elements from %r in %s. This "
                              "ran at about the same speed as the builtin map.",
                              proc_cnt, fname, fmt_dur(total_dur))
            elif self.exec_policy == ExecPolicy.LOCAL:
                self.log.info("Processed %d elements from %r in %s. This "
                              "ran slower than the map builtin by ~%s. "
                              "Consider not using fastmap here.",
                              proc_cnt, fname, fmt_dur(total_dur),
                              fmt_dur(time_saved * -1))
            else:
                self.log.info("Processed %d elements from %r in %s. "
                              "This ran slower than the map builtin by ~%s. "
                              "Consider connecting to a faster "
                              "internet, reducing your data size, or using "
                              "exec_policy LOCAL or ADAPTIVE.",
                              proc_cnt, fname, fmt_dur(total_dur),
                              fmt_dur(time_saved * -1))

        if total_credits_used:
            self.log.info("Spent $%.4f.", total_credits_used / 100)
        self.log.info("Fastmap done.")


def chunk_bytes(payload: bytes, size: int) -> list:
    return [payload[i:i + size] for i in range(0, len(payload), size)]


class _CloudSupervisor(multiprocessing.Process):
    """
    Manages all communication with the cloud service.
    """
    def __init__(self, pickled_func: bytes, itdm: InterThreadDataManager,
                 config: FastmapConfig, label: str):
        multiprocessing.Process.__init__(self)

        local_sources, installed_mods, requirements = get_dependencies(
            config.requirements, config.log)
        encoded_func = msgpack.dumps({
            'func': pickled_func,
            'local_sources': local_sources,
            'installed_mods': installed_mods,
            'requirements': requirements})
        self.func_payload = gzip.compress(encoded_func, compresslevel=1)
        self.func_hash = get_func_hash(self.func_payload)
        self.itdm = itdm

        self.log = config.log
        self.max_cloud_workers = config.max_cloud_workers
        self.cloud_url = config.cloud_url
        self.secret = config.secret
        self.process_name = multiprocessing.current_process().name
        self.label = label
        self.run_id = secrets.token_hex(8)
        self.log.info("Started cloud supervisor for remote url (%r). "
                      "Function payload size is %s.",
                      self.cloud_url, fmt_bytes(len(self.func_payload)))

    def init_remote(self):
        """
        Get the function and modules uploaded to the cloud via the
        /api/v1/init endpoint. This must happen BEFORE calling /api/v1/map.
        Because of server-side caching, and the potential for very large
        payloads, check with the function hash before uploading the function.
        """

        # Step 1: Try just uploaded the function hash. If it exists, we are good.
        req_dict = {}
        req_dict['func_hash'] = self.func_hash
        url = self.cloud_url + "/api/v1/init"
        payload = msgpack.dumps(req_dict)
        resp = post_request(url, payload, self.secret, self.log)

        if resp.status_code != 200:
            raise CloudError("Cloud initialization failed %r." % resp.obj)
        if resp.status == InitStatus.FOUND:
            self.log.info("Function already exists on cloud.")
            return
        if resp.status != InitStatus.NOT_FOUND:
            raise CloudError("Unexpected init status %r." % resp.obj)

        # Step 2: If the server can't find the func, we need to upload it
        # We might need to chunk the upload due to cloud run limits
        func_parts = chunk_bytes(self.func_payload, 20 * MB)
        for i, func_part in enumerate(func_parts):
            req_dict['func'] = func_part
            req_dict['payload_part'] = i
            req_dict['payload_len'] = len(func_parts)
            payload = msgpack.dumps(req_dict)
            payload_bytes = fmt_bytes(len(payload))
            if len(func_parts) > 1:
                self.log.info("Uploading code (%s) part %d/%d..." %
                              (payload_bytes, i + 1, len(func_parts)))
            else:
                self.log.info("Uploading code (%s)..." % payload_bytes)
            resp = post_request(url, payload, self.secret, self.log)

            if resp.status_code != 200:
                raise CloudError("Cloud initialization failed %r." % resp.obj)
            if resp.status == InitStatus.UPLOADED:
                continue
            raise CloudError("Cloud initialization failed. Function not uploaded.")
        self.log.info("Done uploading code.")
        return

    def run(self):
        """
        First, run one cloud batch to manage the possibility of glaring
        cloud-only errors. If that one works, open a number of threads to
        push more to the cloud.
        """
        try:
            self.init_remote()
        except CloudError as e:
            self.log.error("In cloud supervisor during init [%s]: %r.",
                           multiprocessing.current_process().name, e)
            return

        batch_tup = self.itdm.checkout()
        if not batch_tup:
            return

        map_url = self.cloud_url + '/api/v1/map'
        try:
            process_cloud_batch(self.itdm, batch_tup, map_url,
                                self.func_hash, self.label,
                                self.run_id, self.secret, self.log)
        except CloudError as e:
            self.itdm.put_error(self.process_name, repr(e), batch_tup)
            if hasattr(e, 'tb') and e.tb:
                tb = e.tb.replace('%0A', '\n')
                self.log.error("In cloud supervisor [%s]:\n%s.",
                               multiprocessing.current_process().name, tb)
            else:
                self.log.error("In cloud supervisor [%s]: %r.",
                               multiprocessing.current_process().name, e)
            self.log.error("Shutting down cloud processing due to error...")
            return

        threads = []
        num_cloud_threads = min(self.max_cloud_workers,
                                self.itdm.inbox_len())
        self.log.debug("Opening %d cloud connection(s)...", num_cloud_threads)
        for thread_id in range(num_cloud_threads):
            thread_args = (thread_id, map_url, self.func_hash, self.label,
                           self.run_id, self.itdm, self.secret, self.log)
            thread = threading.Thread(target=cloud_thread, args=thread_args)
            thread.start()
            threads.append(thread)
            time.sleep(.1)  # not too many threads opening at once

        for thread in threads:
            thread.join()
        self.log.info("Shutting down cloud processing...")

        post_done_args = (self.cloud_url, self.secret, self.log,
                          self.func_hash, self.run_id)
        done_thread = threading.Thread(target=post_done, args=post_done_args)
        done_thread.start()
