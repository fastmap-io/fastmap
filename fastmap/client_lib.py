"""
Primary file for the fastmap SDK. Almost all client-side code is in this file.
Do not instantiate anything here directly. Use the interface __init__.py.
"""

import base64
import distutils.sysconfig
import gzip
import hashlib
import hmac
import json
import multiprocessing
import os
import re
import secrets
import sys
import threading
import time
import traceback
import types

import dill
import requests

CLOUD_URL_BASE = 'https://fastmap.io'
SECRET_LEN = 64
EXECUTION_ENV = "LOCAL"
CLIENT_VERSION = "0.0.4"
UNSUPPORTED_TYPE_STRS = ('numpy', 'pandas')
SUPPORTED_TYPES = (list, range, tuple, types.GeneratorType)


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
    :param str secret: The API token generated on fastmap.io. Treat this like a
        password. Do not commit it to version control! Failure to do so could
        result in man-in-the-middle attacks or your credits being used by others
        (e.g. cryptocurrency miners). If None, fastmap will run locally
        regardless of exec_policy.
    :param str verbosity: 'QUIET', 'NORMAL', or 'LOUD'. Default is 'NORMAL'.
    :param str exec_policy: 'LOCAL', 'CLOUD', or 'ADAPTIVE'.
        Default is 'ADAPTIVE'.
    :param int local_processes: How many local processes to start. By default,
        fastmap will start a maximum of num_cores * 2 - 2.
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

        fastmap.global_init(secret=FASTMAP_TOKEN)
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

        fastmap_config = fastmap.init(secret=FASTMAP_TOKEN)
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


def set_docstring(docstr, docstr_prefix=''):
    """ Add the given doc string to each function """
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


def fmt_bytes(num_bytes):
    """ Returns human-readable bytes string """
    if num_bytes >= 1024**3:
        return "%.1fGB" % (num_bytes / 1024**3)
    if num_bytes >= 1024**2:
        return "%.1fMB" % (num_bytes / 1024**2)
    if num_bytes >= 1024:
        return "%.1fKB" % (num_bytes / 1024)
    return "%.1fB" % num_bytes


def fmt_time(num_secs):
    hours, remainder = divmod(num_secs, 3600)
    mins, secs = divmod(remainder, 60)
    if hours > 0:
        return '{:02}:{:02}:{:02}'.format(int(hours), int(mins), int(secs))
    if mins > 0:
        return '{:02}:{:02}'.format(int(mins), int(secs))
    return '{}s'.format(int(secs))


def fmt_dur(num_secs):
    if num_secs >= 3600:
        return "%.2f hours" % (num_secs / 3600)
    if num_secs >= 60:
        return "%.2f minutes" % (num_secs / 60)
    if num_secs >= 1:
        return "%.2f seconds" % (num_secs)
    return "%d milliseconds" % (round(num_secs * 1000))


def get_credits(seconds, bytes_egress):
    return 8 * (seconds * 10.0 / 3600.0 + bytes_egress * 10.0 / 1073741824.0)


# DO NOT REMOVE
# def get_func_hash(func):
#     # TODO: I don't like this. I don't know whether it can actually be used
#     # for caching. If upsteam functions change, it will return the same hash.
#     # Thus, we could get false positives and it will be unusable as a caching
#     # object
#     # Maybe after doing pickle.loads in the jailed process, we stop and do
#     # a checksum on the image. If the image checksum is in the cache,
#     # we know we have the same function and can avoid pip installs.
#     # ^ That is option 1. Option 2 is to make dill.dumps deterministic.
#     # I would lean towards option 1 because that would solve the issue of
#     # installs at the same time.
#     # For now, this is not needed for data science functionality so I'll drop it.
#     lines, ln = inspect.getsourcelines(func)
#     source_obj = "".join(lines) + str(ln)
#     return hashlib.sha256(source_obj.encode()).hexdigest()[:16]


class EveryProcessDead(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """


class CloudError(Exception):
    """
    Thrown when the cloud connection results in a non-200
    """
    def __init__(self, *args, **kwargs):
        try:
            self.tb = kwargs.pop('tb')
        except KeyError:
            self.tb = None
        super().__init__(*args, **kwargs)


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


Verbosity = Namespace("QUIET", "NORMAL", "LOUD")
ExecPolicy = Namespace("ADAPTIVE", "LOCAL", "CLOUD")
ReturnType = Namespace("ELEMENTS", "BATCHES")
Color = Namespace(
    GREEN="\033[92m",
    RED="\033[91m",
    YELLOW="\033[93m",
    CYAN="\033[36m",
    CANCEL="\033[0m")


def simplified_tb(tb):
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


def func_name(func):
    """Safe way to get the name of a random function"""
    try:
        name = func.__name__
    except AttributeError:
        name = repr(func)
    name = name[:40] + "..." if len(name) >= 45 else name
    return name


def process_local(func, itdm, log):
    func = dill.loads(func)
    try:
        batch_tup = itdm.checkout()
        while batch_tup:
            batch_idx, batch_iter = batch_tup
            start = time.perf_counter()
            ret = list(map(func, batch_iter))
            total_proc_time = time.perf_counter() - start
            runtime = total_proc_time / len(ret)
            log.debug("Batch %d local cnt=%d dur=%.2fs (%.2e per el)",
                      batch_idx, len(ret), total_proc_time, runtime)
            itdm.push_outbox(batch_idx, ret, runtime)
            batch_tup = itdm.checkout()
    except Exception as e:
        proc_name = multiprocessing.current_process().name
        itdm.put_error(proc_name, repr(e), batch_tup)
        tb = simplified_tb(traceback.format_exc())
        log.error("In local process [%s]:\n %s",
                  multiprocessing.current_process().name, tb)
        return


def auth_token(secret):
    return secret[:SECRET_LEN // 2]


def sign_token(secret):
    return secret[SECRET_LEN // 2:]


def hmac_digest(secret, payload):
    return hmac.new(sign_token(secret).encode(), payload,
                    digestmod=hashlib.sha256).hexdigest()


def create_headers(secret, payload, func_hash, label, run_id):
    """Get headers for communicating with the fastmap.io cloud service"""
    return {
        'Authorization': 'Bearer ' + auth_token(secret),
        'Content-Encoding': 'gzip',
        'X-Python-Version': sys.version.replace('\n', ''),
        'X-Client-Version': CLIENT_VERSION,
        'X-Content-Signature': hmac_digest(secret, payload),
        'X-Func-Hash': func_hash,
        'X-Label': label,
        'X-Run-ID': run_id,
    }


def check_unpickle_resp(resp, secret, maybe_json=False):
    try:
        if 'X-Content-Signature' not in resp.headers:
            raise CloudError("Cloud payload was not signed (%d). "
                             "Will not unpickle." % (resp.status_code))
        cloud_hash = hmac_digest(secret, resp.content)
        if resp.headers['X-Content-Signature'] != cloud_hash:
            raise CloudError("Cloud checksum did not match. Will not unpickle.")
        return dill.loads(base64.b64decode(gzip.decompress(resp.content)))
    except (CloudError, dill.UnpicklingError, gzip.BadGzipFile) as e:
        if maybe_json:
            try:
                return json.loads(resp.content)
            except json.JSONDecodeError:
                pass
        raise e


def process_cloud_batch(itdm, batch_tup, url, req_dict, label, run_id,
                        secret, log):
    start_req_time = time.perf_counter()

    batch_idx, batch = batch_tup
    req_dict['batch'] = batch
    pickled = dill.dumps(req_dict)
    encoded = base64.b64encode(pickled)
    payload = gzip.compress(encoded, compresslevel=1)
    headers = create_headers(secret, payload, req_dict['func_hash'],
                             label, run_id)
    log.debug("Making cloud request len=%d payload=%s (%s per el)",
              len(batch), fmt_bytes(len(payload)),
              fmt_bytes(len(payload) / len(batch)))
    try:
        resp = requests.post(url, data=payload, headers=headers)
    except requests.exceptions.ConnectionError:
        raise CloudError("Fastmap could not connect to %r. "
                         "Check your connection." % url) from None

    if resp.status_code == 200:
        resp_dict = check_unpickle_resp(resp, secret)
        results = resp_dict['results']
        cloud_cid = resp.headers['X-Container-Id']
        cloud_tid = resp.headers['X-Thread-Id']
        if 'X-Server-Warning' in resp.headers:
            # deprecations or anything else
            log.warning(resp.headers['X-Server-Warning'])

        total_request = time.perf_counter() - start_req_time
        total_application = float(resp.headers['X-Application-Seconds'])
        total_mapping = float(resp.headers['X-Map-Seconds'])
        credits_used = float(resp.headers['X-Credits'])
        req_time_per_el = total_request / len(results)
        app_time_per_el = total_application / len(results)
        map_time_per_el = total_mapping / len(results)

        log.debug("Batch %d cloud cnt=%d "
                  "%.2fs/%.2fs/%.2fs map/app/req (%.2e/%.2e/%.2e per el) "
                  "[%s/%s]",
                  batch_idx, len(results),
                  total_mapping, total_application, total_request,
                  map_time_per_el, app_time_per_el, req_time_per_el,
                  cloud_cid, cloud_tid)
        itdm.push_outbox(batch_idx,
                         results,
                         None,
                         credits_used=credits_used,
                         network_seconds=total_request - total_application)
        return

    if resp.status_code == 400:
        # ERROR
        resp_out = check_unpickle_resp(resp, secret)
        raise CloudError("Your code could not be processed on the cloud: %r" %
                         resp_out['exception'], tb=resp_out.get('traceback', ''))
    if resp.status_code == 401:
        # UNAUTHORIZED
        raise CloudError("Unauthorized. Check your API token.")
    if resp.status_code == 402:
        # NOT_ENOUGH_CREDITS
        resp_out = check_unpickle_resp(resp, secret, maybe_json=True)
        raise CloudError("Insufficient credits for this request. "
                         "Your current balance is %.2f credits." %
                         resp_out.get('credits_used', 0))
    if resp.status_code == 403:
        # INVALID_SIGNATURE
        raise CloudError(check_unpickle_resp(resp, secret, maybe_json=True))
    if resp.status_code == 410:
        # DISCONTINUED (post-deprecated end-of-life)
        resp_out = check_unpickle_resp(resp, secret, maybe_json=True)
        raise CloudError("Fastmap.io error: %r" % resp_out['reason'])

    # catch all (should just be for 500s of which a few are explicitly defined)
    try:
        resp_out = check_unpickle_resp(resp, secret, maybe_json=True)
    except (CloudError, dill.UnpicklingError):
        resp_out = resp.content
    raise CloudError("Unexpected cloud error %d %r" %
                     (resp.status_code, resp_out))


def cloud_thread(thread_id, url, req_dict, label, run_id, itdm, secret, log):
    batch_tup = itdm.checkout()
    if batch_tup:
        log.debug("Starting cloud thread %d", thread_id)
    while batch_tup:
        try:
            process_cloud_batch(itdm, batch_tup, url, req_dict,
                                label, run_id, secret, log)
        except CloudError as e:
            proc_name = multiprocessing.current_process().name
            thread_id = threading.get_ident()
            error_loc = "%s: thread:%d" % (proc_name, thread_id)
            itdm.put_error(error_loc, repr(e), batch_tup)
            if hasattr(e, 'tb'):
                log.error("In cloud process [%s]:\n%s",
                          threading.current_thread().name, e.tb)
            else:
                log.error("In cloud process [%s]: %r",
                          threading.current_thread().name, e)
            return

        batch_tup = itdm.checkout()


def get_module_source():  # noqa
    site_packages_re = re.compile(r"\/python[0-9.]+\/(?:site|dist)\-packages\/")
    std_lib_dir = distutils.sysconfig.get_python_lib(standard_lib=True)
    module_source = {}
    for mod_name, mod in sys.modules.items():
        if (mod_name in sys.builtin_module_names or  # not builtin # noqa
            mod_name.startswith("_") or  # not hidden # noqa
            not getattr(mod, '__file__', None) or   # also not builtin # noqa
            mod.__file__.startswith(std_lib_dir) or  # not stdlib # noqa
            not mod.__file__.endswith('.py') or  # not c adapter # noqa
            site_packages_re.search(mod.__file__) or  # not installed # noqa
            (hasattr(mod, "__package__") and  # noqa
             mod.__package__ in ("fastmap", "fastmap.fastmap"))):  # not fastmap
                # TODO, eventually bring in site_packages as well # noqa
                continue  # noqa
        with open(mod.__file__) as f:
            source = f.read()
        module_source[mod.__name__] = source
    return module_source


def post_done(cloud_url_base, secret, log, func_hash, run_id):
    url = cloud_url_base + '/api/v1/done'
    headers = {
        'Authorization': 'Bearer ' + auth_token(secret),
        'X-Python-Version': sys.version.replace('\n', ''),
        'X-Client-Version': CLIENT_VERSION,
        'Content-Type': 'application/json',
    }
    payload = {
        "func_hash": func_hash,
        "run_id": run_id
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    if resp.status_code != 200:
        log.error("Error when wrapping up execution %r", resp.content)


class _CloudProcessor(multiprocessing.Process):
    def __init__(self, func_hash, pickled_func, itdm, config, label):
        multiprocessing.Process.__init__(self)
        self.itdm = itdm
        self.func_hash = func_hash
        self.encoded_func = pickled_func
        self.modules = get_module_source()

        self.log = config.log
        self.max_cloud_connections = config.max_cloud_connections
        self.cloud_url_base = config.cloud_url_base
        self.secret = config.secret
        self.process_name = multiprocessing.current_process().name
        self.label = label
        self.run_id = secrets.token_hex(8)
        self.log.debug("Initialized cloud processor at %r. Func payload is %dB",
                       self.cloud_url_base, len(self.encoded_func))

    def run(self):
        threads = []

        batch_tup = self.itdm.checkout()
        req_dict = {
            'func': self.encoded_func,
            'func_hash': self.func_hash,
            'modules': self.modules,
        }
        url = self.cloud_url_base + '/api/v1/map'

        try:
            process_cloud_batch(self.itdm, batch_tup, url, req_dict, self.label,
                                self.run_id, self.secret, self.log)
        except CloudError as e:
            self.itdm.put_error(self.process_name, repr(e), batch_tup)
            if hasattr(e, 'tb') and e.tb:
                self.log.error("In cloud process manager [%s]:\n%s",
                               multiprocessing.current_process().name, e.tb)
            else:
                self.log.error("In cloud process manager [%s]: %r",
                               multiprocessing.current_process().name, e)
            return

        num_cloud_threads = min(self.max_cloud_connections,
                                self.itdm.inbox_len())
        self.log.debug("Opening %d cloud connection(s)", num_cloud_threads)
        for thread_id in range(num_cloud_threads):
            thread_args = (thread_id, url, req_dict, self.label, self.run_id,
                           self.itdm, self.secret, self.log)
            thread = threading.Thread(target=cloud_thread, args=thread_args)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        post_done_args = (self.cloud_url_base, self.secret, self.log,
                          self.func_hash, self.run_id)
        done_thread = threading.Thread(target=post_done, args=post_done_args)
        done_thread.start()


class InterThreadDataManager():
    """
    Does task allocation between various threads both local and remote.
    """
    def __init__(self, first_runtime, max_batches_in_queue):
        manager = multiprocessing.Manager()
        self._lock = multiprocessing.Lock()
        self._inbox = manager.list()
        self._outbox = manager.list()
        self._runtimes = manager.list()
        self._errors = multiprocessing.Queue()

        self._runtimes.append(first_runtime)

        self.state = manager.dict()
        self.state['inbox_capped'] = False
        self.state['in_tot'] = 0
        self.state['out_tot'] = 0
        self.state['total_credits_used'] = 0
        self.state['total_network_seconds'] = 0.0

    def inbox_len(self):
        if not self.state['inbox_capped']:
            return sys.maxsize
        return self.state['in_tot'] - self.state['out_tot']

    def has_more_batches(self):
        while True:
            if self.state['in_tot'] or self.state['inbox_capped']:
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

    def put_error(self, error_loc, error_str, batch_tup):
        self._errors.put((error_loc, error_str))
        with self._lock:
            self._inbox.insert(0, batch_tup)

    def kill(self):
        with self._lock:
            self.state['inbox_capped'] = True
            self._inbox = None
            self._outbox = None
            self._runtimes = None

    def mark_inbox_capped(self):
        with self._lock:
            assert not self.state['inbox_capped']
            self.state['inbox_capped'] = True
            self.state['inbox_started'] = True

    def push_inbox(self, item):
        with self._lock:
            self._inbox.append(item)
            self.state['in_tot'] += 1

    def total_avg_runtime(self):
        return sum(self._runtimes) / len(self._runtimes)

    def checkout(self):
        # Repeatedly try popping an item off the inbox.
        # If not capped, keep trying.

        while True:
            if self.state['inbox_capped']:
                # If capped, assume that if we don't get an item from the inbox
                # that means we are exhausted and send the poison pill
                with self._lock:
                    try:
                        return self._inbox.pop(0)
                    except IndexError:
                        return None

            with self._lock:
                try:
                    ret = self._inbox.pop(0)
                    return ret
                except IndexError:
                    pass
            time.sleep(.01)

    def push_outbox(self, item_idx, result, runtime,
                    credits_used=None, network_seconds=None):
        with self._lock:
            self._outbox.append((item_idx, result))
            self.state['out_tot'] += 1
            if runtime:
                self._runtimes.append(runtime)
            if credits_used:
                self.state['total_credits_used'] += credits_used
            if network_seconds:
                self.state['total_network_seconds'] += network_seconds

    def pop_outbox(self, processors):
        while True:
            with self._lock:
                if len(self._outbox):
                    return self._outbox.pop(0)
            if not any(p.is_alive() for p in processors):
                ex_str = "While trying to get next processed element"
                raise EveryProcessDead(ex_str)
            time.sleep(.01)


class _FillInbox(threading.Thread):
    BATCH_DUR_GOAL = .2

    def __init__(self, iterable, itdm, log, avg_runtime, *args, **kwargs):
        self.iterable = iterable
        self.itdm = itdm
        self.log = log
        self.avg_runtime = avg_runtime
        self.cnt = 0
        self.do_die = False
        self.capped = False
        super().__init__(*args, **kwargs)

    def kill(self):
        self.do_die = True

    def batch_len(self):
        return max(1, int(self.BATCH_DUR_GOAL / self.avg_runtime))


def batcher(iterable, size):
    """
    We might be able to do this faster.
    Originally, I was using a recipe adapted from here:
    https://stackoverflow.com/questions/8290397/
    how-to-split-an-iterable-in-constant-size-chunks
    However, that failed when we have falsy raw values
    in the iterable (e.g. 0, None, False, etc.)
    I DO have a test for that now: test_reversed_range
    """
    batch = []
    try:
        while True:
            while len(batch) < size:
                batch.append(next(iterable))
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

        for batch in batcher(self.iterable, self.batch_len()):
            self.itdm.push_inbox((batch_cnt, batch))
            batch_cnt += 1
            element_cnt += len(batch)
            if self.do_die:
                return
        self.log.debug("Done adding iterator to task inbox. "
                       "%d batch(es). %d elements", batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped()


class FillInboxWithSeq(_FillInbox):
    """
    For sequences like lists and tuples, this fills the ITDM inbox in a thread
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
        self.log.debug("Done adding list to task inbox. "
                       "%d batch(es). %d elements", batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped()


class FastmapLogger():
    """
    This exists b/c it is difficult to pass python's native logger between
    processes and was requiring a lot of weird workarounds
    """
    def __init__(self, verbosity):
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
        else:
            raise AssertionError(f"Unknown verbosity '{self.verbosity}'")

    def hush(self):
        self.debug = self._do_nothing  # noqa
        self.info = self._do_nothing  # noqa
        self.warning = self._do_nothing  # noqa
        self.error = self._do_nothing  # noqa

    def _do_nothing(self, *args):
        # This instead of a lambda because of pickling in multiprocessing
        pass

    @staticmethod
    def _debug(msg, *args):
        msg = msg % args
        print(Color.CYAN + "fastmap DEBUG:" + Color.CANCEL, msg)

    @staticmethod
    def _info(msg, *args):
        msg = msg % args
        print(Color.YELLOW + "fastmap INFO:" + Color.CANCEL, msg)

    @staticmethod
    def _warning(msg, *args):
        msg = msg % args
        print(Color.RED + "fastmap WARNING:" + Color.CANCEL, msg)

    @staticmethod
    def _error(msg, *args):
        msg = msg % args
        print(Color.RED + "fastmap ERROR:" + Color.CANCEL, msg, flush=True)

    @staticmethod
    def input(msg):
        # This exists mostly for test mocking
        return input("\n fastmap: " + msg)


def seq_progress(gen, seq_len, start_time):
    proc_cnt = 0
    for batch in gen:
        proc_cnt += len(batch)
        yield batch, proc_cnt
        percent = proc_cnt / seq_len * 100
        elapsed_time = time.perf_counter() - start_time
        num_left = seq_len - proc_cnt
        time_remaining = elapsed_time * num_left / proc_cnt
        graph_bar = '█' * int(percent / 2)
        sys.stdout.write("fastmap: %s%s %.1f%% (%s remaining)\r%s" %
                         (Color.GREEN, graph_bar, percent,
                          fmt_time(time_remaining), Color.CANCEL))
        sys.stdout.flush()


def iter_progress(gen, *args):
    proc_cnt = 0
    for batch in gen:
        proc_cnt += len(batch)
        yield batch, proc_cnt
        sys.stdout.write("fastmap: Processed %d\r" % proc_cnt)
        sys.stdout.flush()


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
        "local_processes",
        "max_cloud_connections",
        "cloud_url_base",
    ]

    def __init__(self, secret=None, verbosity=Verbosity.NORMAL,
                 exec_policy=ExecPolicy.ADAPTIVE, confirm_charges=False,
                 local_processes=None, cloud_url_base=CLOUD_URL_BASE):
        if exec_policy not in ExecPolicy:
            raise AssertionError(f"Unknown exec_policy '{exec_policy}'")
        self.exec_policy = exec_policy
        self.log = FastmapLogger(verbosity)
        self.verbosity = verbosity
        self.cloud_url_base = cloud_url_base
        self.confirm_charges = confirm_charges
        self.max_cloud_connections = 20

        if multiprocessing.current_process().name != "MainProcess":
            # Fixes issue with multiple loud inits during local multiprocessing
            # in Mac / Windows
            # TODO: I don't recall why this was needed and can't seem to make
            # a test case. Possible removal candidate...
            self.log.hush()

        if secret:
            assert len(secret) == SECRET_LEN
            self.secret = secret
        else:
            self.secret = None
            if self.exec_policy != ExecPolicy.LOCAL:
                self.exec_policy = ExecPolicy.LOCAL
                self.log.warning("No secret provided. "
                                 "Setting exec_policy to LOCAL.")

        if local_processes:
            self.local_processes = local_processes
        else:
            self.local_processes = os.cpu_count() - 2

        if not confirm_charges and self.exec_policy != ExecPolicy.LOCAL:
            self.log.warning("Your fastmap credit balance will be "
                             "automatically debited for use. To avoid "
                             "automatic debits, set confirm_charges=True")

        self.log.debug("Setup fastmap")
        self.log.debug(" verbosity: %s", self.verbosity)
        self.log.debug(" exec_policy: %s", self.exec_policy)
        if exec_policy != ExecPolicy.CLOUD:
            self.log.debug(" local_processes: %d", self.local_processes)
        self.log.restore_verbosity()  # undo hush

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable, return_type=ReturnType.ELEMENTS, label=""):
        if return_type not in ReturnType:
            raise AssertionError(f"Unknown return_type '{return_type}'")
        iter_type = str(type(iterable))
        if any(t in iter_type for t in UNSUPPORTED_TYPE_STRS):
            self.log.warning(f"Type '{iter_type}' is explicitly not supported")
        elif not any(isinstance(iterable, t) for t in SUPPORTED_TYPES):
            self.log.warning(f"Type '{iter_type}' is not explictly supported")

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
            if self.verbosity == Verbosity.QUIET:
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
            progress_func = seq_progress if is_seq else iter_progress

            if return_type == ReturnType.BATCHES:
                for batch, proc_cnt in progress_func(fm, seq_len, start_time):
                    yield batch
            else:
                # else return_type is ELEMENTS
                for batch, proc_cnt in progress_func(fm, seq_len, start_time):
                    for el in batch:
                        yield el

        except Exception as e:
            if mapper.itdm:
                try:
                    while True:
                        process_error = mapper.itdm.get_error()
                        errors.append(" Process error [%s]: %s" % (
                                      process_error[0], process_error[1]))
                except multiprocessing.queues.Empty:
                    pass
            mapper.cleanup()

            if not errors or not isinstance(e, EveryProcessDead):
                raise e

        if errors:
            error_msg = "Every execution process died. List of errors:"
            for error in errors:
                error_msg += "\n  " + error
            self.log.error(error_msg)
            raise EveryProcessDead(error_msg)

        total_dur = time.perf_counter() - start_time
        self._log_final_stats(func_name(func), mapper, proc_cnt,
                              total_dur)

    def _log_final_stats(self, fname, mapper, proc_cnt, total_dur):
        avg_runtime = mapper.avg_runtime
        total_credits_used = mapper.total_credits_used

        print()
        if not avg_runtime:
            self.log.info("Done processing %r in %.2fms." % (fname, total_dur*1000))
        else:
            time_saved = avg_runtime * proc_cnt - total_dur
            if time_saved > 0.02:
                self.log.info("Processed %d elements from %r in %s. "
                              "You saved ~%s", proc_cnt, fname,
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
            self.log.info("Used %.3f credits.", total_credits_used)


class Mapper():
    """
    Wrapper for running fastmap.
    Stores execution state which is separate from the overall configuration.
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

        self.processors = []
        self.fill_inbox_thread = None
        self.itdm = None

    def cleanup(self):
        self.log.info("Cleaning up threads and processes because of error...")
        for p in self.processors:
            p.join()
        if self.fill_inbox_thread:
            try:
                self.fill_inbox_thread.kill()
            finally:
                self.fill_inbox_thread.join()
        if self.itdm:
            self.itdm.kill()
        self.log.info("Threads and processes clean.")

    def _confirm_charges(self, iterable):
        if not self.config.confirm_charges:
            return True
        while True:
            if hasattr(iterable, "__len__"):
                credit_estimate = get_credits(self.avg_runtime * len(iterable),
                                              self.avg_egress * len(iterable))
                user_input_query = "Estimate: %.3f credits. Continue?" % \
                    credit_estimate
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
                             "Please input 'y' or 'n'", user_input)

    def _get_processors(self, func, iterable, label):
        # TODO make this smarter
        if self.config.exec_policy == ExecPolicy.CLOUD:
            max_batches_in_queue = self.config.max_cloud_connections
        elif self.config.exec_policy == ExecPolicy.LOCAL:
            max_batches_in_queue = self.config.local_processes
        else:
            max_batches_in_queue = self.config.local_processes + \
                                   self.config.max_cloud_connections  # noqa

        itdm = InterThreadDataManager(self.avg_runtime, max_batches_in_queue)

        pickled_func = dill.dumps(func, recurse=True)
        processors = []
        if self.config.exec_policy != ExecPolicy.CLOUD:
            for _ in range(self.config.local_processes):
                proc_args = (pickled_func, itdm, self.log)
                local_proc = multiprocessing.Process(target=process_local,
                                                     args=proc_args)
                local_proc.start()
                processors.append(local_proc)
            self.log.debug("Launching %d local processes",
                           self.config.local_processes)
        if self.config.exec_policy != ExecPolicy.LOCAL:
            if self._confirm_charges(iterable):
                func_hash = "TODO"
                cloud_proc = _CloudProcessor(func_hash, pickled_func, itdm,
                                             self.config, label)
                cloud_proc.start()
                processors.append(cloud_proc)

        if not processors or not any(p.is_alive() for p in processors):
            raise EveryProcessDead("No execution processes started. "
                                   "Try modifying your fastmap configuration.")

        return processors, itdm

    def initial_run(self, func, iterable):
        """
        Maps the function over the iterable for a short period of time to
        estimate the avg runtime which will is used to make decisions about
        the best way to execute the map (via multiprocessing, remotely,
        batch_size, etc.)
        :returns: the processed results, possibly more (but not certainly)
        :rtype: list, stop
        """
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
                self.log.debug("Initial run processed entire iterable")
                return ret, False
            ret.append(func(item))
            if time.perf_counter() > end_loop_at:
                break
        first_batch_run_time = time.perf_counter() - start_time
        len_first_batch = len(ret)
        self.avg_runtime = first_batch_run_time / len_first_batch
        self.avg_egress = len(dill.dumps(ret)) / len_first_batch
        self.log.debug("Processed first %d elements in %.2fs (%e per element)",
                       len_first_batch, first_batch_run_time, self.avg_runtime)
        return ret, True

    def _estimate_multiproc_runtime(self, iterable):
        local_runtime = self.avg_runtime * len(iterable)
        total_proc_overhead = self.config.local_processes * self.PROC_OVERHEAD
        total_process_runtime = local_runtime / self.config.local_processes
        return total_proc_overhead + total_process_runtime

    def map(self, func, iterable, label, is_seq=False):
        if is_seq:
            seq_len = len(iterable)
            self.log.debug("Applying %r to %d items and yielding the results.",
                           func_name(func), seq_len)
        else:
            self.log.debug("Applying %r to a generator and yielding the "
                           "results.", func_name(func))

        initial_batch, maybe_more = self.initial_run(func, iterable)
        yield initial_batch
        if not maybe_more:
            return

        if is_seq:
            # remove already processed batch from iterable
            iterable = iterable[len(initial_batch):]

        if is_seq and self.config.exec_policy != ExecPolicy.CLOUD:
            local_runtime = self.avg_runtime * len(iterable)
            if local_runtime < self._estimate_multiproc_runtime(iterable):
                self.log.debug("Running single-threaded due to "
                               "short expected runtime.")
                yield list(map(func, iterable))
                return

        if self.config.local_processes <= 1 and \
           self.config.exec_policy != ExecPolicy.CLOUD:
            self.log.debug("Running single-threaded due to having <= 1 "
                           "process available.")
            yield list(map(func, iterable))
            return

        self.processors, self.itdm = self._get_processors(func, iterable, label)

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
            result_idx, result_list = self.itdm.pop_outbox(self.processors)
            staging[result_idx] = result_list
            while cur_idx in staging:
                yield staging.pop(cur_idx)
                cur_idx += 1

        # self.log.debug("Cleaning up threads and processes...")
        self.fill_inbox_thread.join()
        for processor in self.processors:
            processor.join()

        self.total_credits_used = self.itdm.get_total_credits_used()
        self.avg_runtime = self.itdm.total_avg_runtime()
        self.total_network_seconds = self.itdm.get_total_network_seconds()
