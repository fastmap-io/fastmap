import base64
import gzip
import hashlib
import hmac
import itertools
import multiprocessing
import os
import pickle
import sys
import threading
import time

import dill
# import psutil
import requests

CLIENT_VERSION = "0.0.1"
CLOUD_URL_BASE = 'https://fastmap.io'

FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the results.
    Depending on prior configuration, fastmap will run either locally via
    multiprocessing, in the cloud on the fastmap.io servers, or adaptively on
    both.

    :param func: Function to map against.
    :param iterable: Iterable to map over.
    :rtype: Generator

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in most circumstances.

    Notes:
    - The function passed in must be stateless and cannot access the network or
      the filesystem. If run locally, these restrictions will not be enforced
      but because fastmap will likely execute out-of-order, running stateful
      functions is not recommended.
    - The iterable may be either a sequence (list, tuple, etc) or a generator.
      Both are processed lazily so fastmap will not begin execution unless
      iterated over or execution is forced (e.g. by wrapping it in a list).

    """

INIT_PARAMS = """
    :param secret: The API token generated on fastmap.io. Treat this like a
        password. Do not commit it to version control! Failure to do so could
        result in man-in-the-middle attacks or your credits being used by others
        (e.g. cryptocurrency miners). If None, fastmap will run locally
        regardless of exec_policy.
    :param verbosity: 'QUIET', 'NORMAL', or 'LOUD'. Default is 'NORMAL'.
    :param exec_policy: 'LOCAL', 'CLOUD', or 'ADAPTIVE'. Default is 'ADAPTIVE'.
    :param all_local_cpus: By default, fastmap will not utilize all available
        CPUs to allow other processes to remain performant. If this parameter
        is set to True, fastmap may run marginally faster.
    :param confirm_charges: Manually confirm cloud charges.
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
    return "%dB" % num_bytes

def fmt_time(num_secs):
    hours, remainder = divmod(num_secs, 3600)
    mins, secs = divmod(remainder, 60)
    if hours > 0:
        return '{:02}:{:02}:{:02}'.format(int(hours), int(mins), int(secs))
    if mins > 0:
        return '{:02}:{:02}'.format(int(mins), int(secs))
    return '{}s'.format(int(secs))

def get_hash(obj):
    return hashlib.sha256(obj).hexdigest()


class ExecutionError(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """

class RemoteError(Exception):
    """
    Thrown when the remote connection results in a non-200
    """

class Namespace(dict):
    def __init__(self, *args, **kwargs):
        d = {k: k for k in args}
        d.update({k: v for k, v in kwargs.items()})
        super(Namespace, self).__init__(d)

    def __getattr__(self, item):
        if item in self:
            return self[item]
        raise AttributeError

Verbosity = Namespace("QUIET", "NORMAL", "LOUD")
ExecPolicy = Namespace("ADAPTIVE", "LOCAL", "CLOUD")
Color = Namespace(GREEN="\033[92m", CANCEL="\033[0m")


def process_local(func, itdm, log):
    try:
        batch_tup = itdm.checkout()
        while batch_tup:
            batch_idx, batch_iter = batch_tup
            start = time.perf_counter()
            ret = list(map(func, batch_iter))
            total_proc_time = time.perf_counter() - start
            runtime = total_proc_time / len(ret)
            log.debug("Batch %d of size %d ran in %.2f (%.2e per element)",
                      batch_idx, len(ret), total_proc_time, runtime)
            itdm.push_outbox(batch_idx, ret, runtime)
            batch_tup = itdm.checkout()
    except Exception as e:
        proc_name = multiprocessing.current_process().name
        itdm.put_error(proc_name, repr(e), batch_tup)
        raise e

def hmac_digest(secret, payload):
    return hmac.new(secret[48:].encode(), payload,
                    digestmod=hashlib.sha256).hexdigest()


def create_headers(secret, payload):
    """Get headers for communicating with the fastmap.io cloud service"""
    return {
        'Authorization': 'Bearer ' + secret[:48],
        'Content-Encoding': 'gzip',
        'X-Python-Version': sys.version.replace('\n', ''),
        'X-Client-Version': CLIENT_VERSION,
        'X-Content-Signature': hmac_digest(secret, payload),
    }


def unpickle_resp(resp):
    return pickle.loads(base64.b64decode(resp.content))


def check_unpickle_resp(resp, secret):
    if 'X-Content-Signature' not in resp.headers:
        raise RemoteError("Remote payload was not signed. Will not unpickle.")
    remote_hash = hmac_digest(secret, resp.content)
    if resp.headers['X-Content-Signature'] != remote_hash:
        raise RemoteError("Remote checksum did not match. Will not unpickle.")
    return unpickle_resp(resp)


def process_remote_batch(itdm, batch_tup, url, req_dict, secret, log):
    start_req_time = time.perf_counter()

    batch_idx, batch = batch_tup
    req_dict['batch'] = batch
    pickled = pickle.dumps(req_dict)
    encoded = base64.b64encode(pickled)
    payload = gzip.compress(encoded, compresslevel=1)
    headers = create_headers(secret, payload)
    log.debug("Making cloud request with %d elements. %s. %.2fB / element ",
              len(batch), fmt_bytes(len(payload)), len(payload) / len(batch))
    try:
        resp = requests.post(url, data=payload, headers=headers)
    except requests.exceptions.ConnectionError:
        raise RemoteError("Fastmap could not connect to the cloud server. "
                          "Check your connection.")
    if resp.status_code == 200:
        resp_dict = check_unpickle_resp(resp, secret)
        results = resp_dict['results']
        remote_cid = resp.headers['X-Container-Id']
        remote_tid = resp.headers['X-Thread-Id']

        total_request = time.perf_counter() - start_req_time
        total_application = float(resp.headers['X-Total-Seconds'])
        total_mapping = resp_dict['map_seconds']
        req_time_per_el = total_request / len(results)
        app_time_per_el = total_application / len(results)
        map_time_per_el = total_mapping / len(results)

        log.debug("Got %d results from %s/%s",
                  len(results), remote_cid, remote_tid)
        log.debug("Batch processed for %.2f/%.2f/%.2f map/app/req "
                  "(%.2e/%.2e/%.2e per element)",
                  total_mapping, total_application, total_request,
                  map_time_per_el, app_time_per_el, req_time_per_el)
        itdm.push_outbox(batch_idx,
                         results,
                         None,
                         vcpu_seconds=total_application,
                         network_seconds=total_request-total_application)
        return

    if resp.status_code == 401:
        raise RemoteError("Unauthorized. Check your API token.")
    if resp.status_code == 403:
        raise RemoteError("Invalid client signature. Check your API token.")
    if resp.status_code == 402:
        resp_out = check_unpickle_resp(resp, secret)
        raise RemoteError("Insufficient vCPU credits for this request. "
                          "Your current balance is %.4f vCPU-seconds." %
                          resp_out.get('vcpu_seconds', 0.0))
    if resp.status_code == 400:
        resp_out = check_unpickle_resp(resp, secret)
        raise RemoteError("Your code could not be processed remotely.\n" +
                          resp_out['traceback'])
    try:
        resp_out = check_unpickle_resp(resp, secret)
    except pickle.UnpicklingError:
        resp_out = resp.content
    raise RemoteError("Unexpected remote error %d %r" %
                      (resp.status_code, resp_out))



def remote_thread(thread_id, url, func_hash, encoded_func,
                             itdm, secret, log):

    batch_tup = itdm.checkout()
    req_dict = {
        'func': encoded_func,
        'func_hash': func_hash,
    }

    while batch_tup:
        try:
            process_remote_batch(itdm, batch_tup, url, req_dict, secret, log)
        except Exception as e:
            proc_name = multiprocessing.current_process().name
            thread_id = threading.get_ident()
            error_loc = "%s: thread:%d" % (proc_name, thread_id)
            itdm.put_error(error_loc, repr(e), batch_tup)
            raise e
        batch_tup = itdm.checkout()


class _RemoteProcessor(multiprocessing.Process):
    def __init__(self, func, itdm, config):
        multiprocessing.Process.__init__(self)
        self.itdm = itdm
        self.func = func

        pickled_func = dill.dumps(func)
        self.func_hash = get_hash(pickled_func)
        self.encoded_func = pickled_func

        self.log = config.log
        self.max_cloud_connections = config.max_cloud_connections
        self.cloud_url_base = config.cloud_url_base
        self.secret = config.secret
        self.process_name = multiprocessing.current_process().name
        self.log.info("Initialized remote processor. Func payload is %dB",
                      len(self.encoded_func))

    def start(self):
        # TODO did I want to use this for something?
        # It does trigger and does so before .run
        super(_RemoteProcessor, self).start()

    def run(self):
        threads = []

        batch_tup = self.itdm.checkout()
        req_dict = {
            'func': self.encoded_func,
            'func_hash': self.func_hash,
        }
        url = self.cloud_url_base + '/api/v1/map'

        try:
            process_remote_batch(self.itdm, batch_tup, url, req_dict,
                                 self.secret, self.log)
        except Exception as e:
            print("except", e)
            self.itdm.put_error(self.process_name, repr(e), batch_tup)
            print("except here")
            return

        self.log.debug("Opening %d remote connection(s)",
                       self.max_cloud_connections)
        for thread_id in range(self.max_cloud_connections):
            self.log.debug("Creating remote thread %r", thread_id)
            thread_args = (thread_id, url, self.func_hash, self.encoded_func,
                           self.itdm, self.secret, self.log)
            thread = threading.Thread(target=remote_thread, args=thread_args)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()



class InterThreadDataManager():

    def __init__(self, iterable, first_runtime, max_batches_in_queue):
        manager = multiprocessing.Manager()
        self._lock = multiprocessing.Lock()
        self._get_lock = multiprocessing.Lock()
        self._inbox = multiprocessing.Queue()
        self._retry = multiprocessing.Queue()
        self._outbox = manager.list()
        self._runtimes = manager.list()
        self._errors = multiprocessing.Queue()

        self._runtimes.append(first_runtime)

        self.state = manager.dict()
        self.state['inbox_capped'] = False
        self.state['in_minus_out'] = 0
        self.state['total_vcpu_seconds'] = 0.0
        self.state['total_network_seconds'] = 0.0

        self.batch_goal_time = 1.0

    def has_more_batches(self):
        if not self.state['inbox_capped']:
            return True
        return self.state['in_minus_out'] > 0

    def get_total_vcpu_seconds(self):
        return self.state['total_vcpu_seconds']

    def get_total_network_seconds(self):
        return self.state['total_network_seconds']

    def get_error(self):
        return self._errors.get_nowait()

    def put_error(self, error_loc, error_str, batch_tup):
        # TODO, this is blocking because of some fundamental size constraints
        # https://bugs.python.org/issue8237
        # Why does _inbox not block (or does it?)
        # Write more tests with very large objects.
        self._errors.put((error_loc, error_str, batch_tup[1][:25000]))
        # self._retry.put(batch_tup)
        # self._inbox.put(batch_tup)

    def kill(self):
        with self._lock:
            self.state['inbox_capped'] = True
            try:
                while True:
                    self._inbox.get_nowait()
            except multiprocessing.queues.Empty:
                pass
            self._outbox = None
            self._runtimes = None

    def mark_inbox_capped(self):
        with self._lock:
            assert not self.state['inbox_capped']
            self.state['inbox_capped'] = True

    def push_inbox(self, item):
        self._inbox.put(item)
        with self._lock:
            self.state['in_minus_out'] += 1

    def total_avg_runtime(self):
        return sum(self._runtimes) / len(self._runtimes)

    def checkout(self):
        # Repeatedly try popping an item off the inbox.
        # If not capped, keep trying.
        try:
            return self._retry.get_nowait()
        except multiprocessing.queues.Empty:
            pass

        while True:
            if self.state['inbox_capped']:
                # If capped, assume that if we don't get an item from the inbox
                # that means we are exhausted and send the poison pill
                try:
                    return self._inbox.get_nowait()
                except multiprocessing.queues.Empty:
                    return None
            try:
                ret = self._inbox.get(timeout=.01)
                return ret
            except multiprocessing.queues.Empty:
                pass

    def push_outbox(self, item_idx, result, runtime,
                    vcpu_seconds=None, network_seconds=None):
        with self._lock:
            self._outbox.append((item_idx, result))
            self.state['in_minus_out'] -= 1
            if runtime:
                self._runtimes.append(runtime)
            if vcpu_seconds:
                self.state['total_vcpu_seconds'] += vcpu_seconds
            if network_seconds:
                self.state['total_network_seconds'] += network_seconds
            # del self._loans[item_idx]

    def pop_outbox(self, processors):
        print("popping", processors)
        while True:
            with self._lock:
                if len(self._outbox):
                    return self._outbox.pop(0)
            if not any(p.is_alive() for p in processors):
                raise ExecutionError("Every execution process died.")
            time.sleep(.01)
            # print("pop true", processors)

    # def reprocess_loans(self):
    #     with self._lock:
    #         self._inbox = sorted(self._loans.items()) + self._inbox
    #         # self._loans = {}

class _FillInbox(threading.Thread):
    def __init__(self, iterable, itdm, log, avg_runtime, *args, **kwargs):
        self.iterable = iterable
        self.itdm = itdm
        self.log = log
        self.avg_runtime = avg_runtime
        self.cnt = 0
        self.do_die = False
        self.capped = False
        super(_FillInbox, self).__init__(*args, **kwargs)

    def kill(self):
        self.do_die = True

    def batch_len(self):
        return max(1, int(.2 / self.avg_runtime))


class FillInboxWithGen(_FillInbox):
    def run(self):
        """
        The itertoos.zip_longest method was adapted from an answer found here:
        https://stackoverflow.com/questions/8290397/
        how-to-split-an-iterable-in-constant-size-chunks
        Validated that this works JIT for slow upsteam generators
        """
        batch_cnt = 0
        element_cnt = 0
        args = [self.iterable] * self.batch_len()
        for batch in map(lambda l: list(filter(None, l)),
                         itertools.zip_longest(*args, fillvalue=None)):
            self.itdm.push_inbox((batch_cnt, batch))
            batch_cnt += 1
            element_cnt += len(batch)
            if self.do_die:
                return
        self.log.debug("Done adding iterator to task inbox. "
                       "%d batch(es). %d elements", batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped()


class FillInboxWithSeq(_FillInbox):
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
        if verbosity == Verbosity.LOUD:
            pass
        elif verbosity == Verbosity.NORMAL:
            self.debug = lambda *args: None
        elif verbosity == Verbosity.QUIET:
            self.debug = lambda *args: None
            self.info = lambda *args: None
        else:
            raise AssertionError(f"Unknown value for verbosity '{verbosity}'")

    def debug(self, msg, *args):
        msg = msg % args
        print("fastmap DEBUG:", msg)

    def info(self, msg, *args):
        msg = msg % args
        print("fastmap INFO:", msg)

    def warning(self, msg, *args):
        msg = msg % args
        print("fastmap WARNING:", msg)

    def error(self, msg, *args):
        msg = msg % args
        print("fastmap ERROR:", msg)

    def input(self, msg):
        # This exists mostly for test mocking
        return input("\n fastmap: " + msg)


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
        "num_local_procs",
        "max_cloud_connections",
        "cloud_url_base",
    ]

    def __init__(self, secret=None, verbosity=Verbosity.NORMAL,
                 exec_policy=ExecPolicy.ADAPTIVE, confirm_charges=False,
                 all_local_cpus=False):
        if exec_policy not in ExecPolicy:
            raise AssertionError(f"Unknown exec_policy '{exec_policy}'")
        self.exec_policy = exec_policy
        self.log = FastmapLogger(verbosity)
        self.verbosity = verbosity
        self.confirm_charges = confirm_charges
        self.num_local_procs = os.cpu_count()
        self.cloud_url_base = CLOUD_URL_BASE
        self.max_cloud_connections = 3

        if secret:
            assert len(secret) == 96
            self.secret = secret
        else:
            self.secret = None
            if self.exec_policy != ExecPolicy.LOCAL:
                self.exec_policy = ExecPolicy.LOCAL
                self.log.warning("No secret provided. "
                                 "Setting exec_policy to LOCAL.")

        # try:
        #     self.num_local_procs = len(psutil.Process().cpu_affinity())
        # except AttributeError:
        #     self.num_local_procs = psutil.cpu_count()
        if not all_local_cpus:
            self.num_local_procs -= 2

        if not confirm_charges and self.exec_policy != ExecPolicy.LOCAL:
            self.log.warning("The parameter 'confirm_charges' is False. Your "
                             "vCPU-hour balance will be automatically debited "
                             "for use.")

        self.log.debug("Setup fastmap")
        self.log.debug(" verbosity: %s.", verbosity)
        self.log.debug(" exec_policy: %s", exec_policy)

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable):
        start_time = time.perf_counter()
        mapper = Mapper(self)
        is_seq = hasattr(iterable, '__len__')

        try:
            if self.verbosity == Verbosity.QUIET:
                for batch in mapper.map(func, iterable, is_seq=is_seq):
                    for el in batch:
                        yield el
                return

            num_processed = 0
            if is_seq:
                seq_len = len(iterable)
                if not seq_len:
                    return
                for batch in mapper.map(func, iterable, is_seq=True):
                    for el in batch:
                        yield el
                    num_processed += len(batch)
                    percent = num_processed / seq_len * 100
                    elapsed_time = time.perf_counter() - start_time
                    num_left = seq_len - num_processed
                    time_remaining = elapsed_time * num_left / num_processed
                    graph_bar = '█' * int(percent / 2)
                    sys.stdout.write("fastmap: %s%s %.1f%% (%s remaining)\r%s" %
                                     (Color.GREEN, graph_bar, percent,
                                      fmt_time(time_remaining), Color.CANCEL))
                    sys.stdout.flush()
            else:
                for batch in mapper.map(func, iterable, is_seq=False):
                    for el in batch:
                        yield el
                    num_processed += len(batch)
                    sys.stdout.write("fastmap: Processed %d\r" % num_processed)
                    sys.stdout.flush()
        except Exception as e:
            if mapper.itdm:
                try:
                    while True:
                        process_error = mapper.itdm.get_error()
                        self.log.error(" Process error [%s]: %s",
                                       process_error[0], process_error[1])
                except multiprocessing.queues.Empty:
                    pass

                try:
                    while True:
                        process_error = mapper.itdm._retry.get_nowait()
                        self.log.error(" batchtup TODO remove [%s]: %s",
                                       process_error[0], process_error[1])
                except multiprocessing.queues.Empty:
                    pass

            mapper.cleanup()
            raise e

        total_duration = time.perf_counter() - start_time
        self._log_final_stats(mapper, num_processed, total_duration)

    def _log_final_stats(self, mapper, num_processed, total_duration):

        avg_runtime = mapper.avg_runtime
        total_network_seconds = mapper.total_network_seconds
        total_vcpu_seconds = mapper.total_vcpu_seconds

        print()
        if not avg_runtime:
            self.log.info("Fastmap processing done.")
        else:
            time_saved = avg_runtime * num_processed - total_duration
            if time_saved > 3600:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f hours",
                              num_processed, total_duration, time_saved / 3600)
            if time_saved > 60:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f minutes",
                              num_processed, total_duration, time_saved / 60)
            elif time_saved > 0.01:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f seconds",
                              num_processed, total_duration, time_saved)
            elif abs(time_saved) < 0.01:
                self.log.info("Processed %d elements in %.2fs. This ran at "
                              "about the same speed as the builtin map.",
                              num_processed, total_duration)
            elif self.exec_policy == ExecPolicy.LOCAL:
                self.log.info("Processed %d elements in %.2fs. This ran slower "
                              "than the map builtin by %.2fs (estimate). "
                              "Consider not using fastmap here.",
                              num_processed, total_duration, time_saved * -1)
            else:
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran slower than the map builtin by %.2fs "
                              "(estimate). Network transfer accounts for %.2fs "
                              "of this duration. Consider upgrading your "
                              "connection, reducing your data size, or using "
                              "exec_policy LOCAL or ADAPTIVE.",
                              num_processed, total_duration, time_saved * -1,
                              total_network_seconds)

        if total_vcpu_seconds:
            self.log.info("Used %.4f vCPU-hours.", total_vcpu_seconds/60/60)


class Mapper():
    """
    Wrapper for running fastmap. Maintains state variables including
    seq_len and avg_runtime.
    """
    INITIAL_RUN_DUR = 0.1  # seconds
    PROC_OVERHEAD = 0.1  # seconds

    def __init__(self, config):
        self.config = config
        self.log = config.log
        self.avg_runtime = None
        self.total_vcpu_seconds = 0
        self.total_network_seconds = 0

        self.processors = []
        self.fill_inbox_thread = None
        self.itdm = None

    def cleanup(self):
        self.log.error("Cleaning up threads and processes because of error...")
        for p in self.processors:
            p.join()
        if self.fill_inbox_thread:
            try:
                self.fill_inbox_thread.kill()
            finally:
                self.fill_inbox_thread.join()
        if self.itdm:
            self.itdm.kill()
        self.log.error("Threads and processes clean.")

    def _confirm_charges(self, iterable):
        if not self.config.confirm_charges:
            return True
        while True:
            if hasattr(iterable, "__len__"):
                # TODO take local processing into account
                vcpu_estimate = fmt_time(self.avg_runtime * len(iterable))
                user_input_query = "Estimated vCPU usage of %s." \
                                   " Continue?" % vcpu_estimate
            else:
                user_input_query = "Cannot estimate vCPU usage because " \
                                   "iterable is a generator. Continue anyway?"
            user_input = self.log.input("%s (y/n) " % user_input_query)
            if user_input.lower() == 'y':
                return True
            elif user_input.lower() == 'n':
                if self.config.exec_policy == ExecPolicy.ADAPTIVE:
                    self.log.info("Cloud operation cancelled. "
                                  "Continuing processing locally...")
                return False
            else:
                self.log.warning("Unrecognized input of %r. "
                                 "Please input 'y' or 'n'", user_input)


    def _get_processors(self, func, iterable):
        # TODO make this smarter
        if self.config.exec_policy == ExecPolicy.CLOUD:
            max_batches_in_queue = self.config.max_cloud_connections
        elif self.config.exec_policy == ExecPolicy.LOCAL:
            max_batches_in_queue = self.config.num_local_procs
        else:
            max_batches_in_queue = self.config.num_local_procs + \
                                   self.config.max_cloud_connections

        itdm = InterThreadDataManager(iterable, self.avg_runtime,
                                      max_batches_in_queue)

        processors = []
        if self.config.exec_policy != ExecPolicy.CLOUD:
            for _ in range(self.config.num_local_procs):
                proc_args = (func, itdm, self.log)
                local_proc = multiprocessing.Process(target=process_local,
                                                     args=proc_args)
                local_proc.start()
                processors.append(local_proc)
            self.log.debug("Launching %d local processes",
                           self.config.num_local_procs)
        if self.config.exec_policy != ExecPolicy.LOCAL:
            if self._confirm_charges(iterable):
                remote_proc = _RemoteProcessor(func, itdm, self.config)
                remote_proc.start()
                processors.append(remote_proc)

        if not processors or not any(p.is_alive() for p in processors):
            raise ExecutionError("No execution processes started. " \
                                 "Try modifying your fastmap configuration.")

        return processors, itdm

    def initial_run(self, func, iterable):
        """
        Maps the function over the iterable for a short period of time to
        estimate the avg runtime which will is used to make decisions about
        the best way to execute the map (via multiprocessing, remotely,
        batch_size, etc.)
        :return: the processed results, more to process
        :rtype: list, bool
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
        self.log.debug("Processed first %d elements in %.2fs (%e per element)",
                       len_first_batch, first_batch_run_time, self.avg_runtime)
        return ret, True

    def _estimate_multiproc_runtime(self, iterable):
        local_runtime = self.avg_runtime * len(iterable)
        total_proc_overhead = self.config.num_local_procs * self.PROC_OVERHEAD
        total_process_runtime = local_runtime / self.config.num_local_procs
        return total_proc_overhead + total_process_runtime


    def map(self, func, iterable, is_seq=False):
        if is_seq:
            seq_len = len(iterable)
            self.log.debug("Applying %r to %d items and yielding the results.",
                           func.__name__, seq_len)
        else:
            self.log.debug("Applying %r to a generator and yielding the "
                           "results.", func.__name__)

        initial_batch, has_more = self.initial_run(func, iterable)
        yield initial_batch
        if not has_more:
            return

        if is_seq:
            iterable = iterable[len(initial_batch):]

        if is_seq and self.config.exec_policy != ExecPolicy.CLOUD:
            local_runtime = self.avg_runtime * len(iterable)
            if local_runtime < self._estimate_multiproc_runtime(iterable):
                self.log.debug("Running single-threaded due to "
                               "short expected runtime")
                yield list(map(func, iterable))
                return

        self.processors, self.itdm = self._get_processors(func, iterable)

        if is_seq:
            self.fill_inbox_thread = FillInboxWithSeq(
                iterable, self.itdm, self.log, self.avg_runtime)
        else:
            self.fill_inbox_thread = FillInboxWithGen(
                iterable, self.itdm, self.log, self.avg_runtime)
        self.fill_inbox_thread.start()

        cur_idx = 0
        staging = {}
        print("here?")
        while self.itdm.has_more_batches():
            result_idx, result_list = self.itdm.pop_outbox(self.processors)
            staging[result_idx] = result_list
            while cur_idx in staging:
                yield staging.pop(cur_idx)
                cur_idx += 1

        self.log.debug("Cleaning up threads and processes...")
        self.fill_inbox_thread.join()
        for processor in self.processors:
            processor.join()

        self.total_vcpu_seconds = self.itdm.get_total_vcpu_seconds()
        self.avg_runtime = self.itdm.total_avg_runtime()
        self.total_network_seconds = self.itdm.get_total_network_seconds()
