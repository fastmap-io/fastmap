import base64
import gzip
import hashlib
import logging
import multiprocessing
import os
import pickle
import sys
import threading
import time

import dill
# import psutil
import requests

_transfer_durs = {}
dill.settings['recurse'] = True


FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the results.
    Depending on prior configuration, fastmap will run either locally via multiprocessing,
    in the cloud on the fastmap.io servers, or adaptively on both.

    :param func: Function to map with. Must be a "pure function" with no side effects.
    :param iterable: Iterable to map over.

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in most circumstances.
    For more documentation on fastmap, visit https://fastmap.io/docs
    """

FASTRUN_DOCSTRING = """
    Start a function and returns a Promise object.
    Depending on prior configuration, fastmap will run either locally via a separate process,
    in the cloud on the fastmap.io servers, or adaptively on both.

    :param func: Function to run. Must be a "pure function" with no side effects.
    :param *args: Positional arguments for the function.
    :param **kwargs: Keyword arguments for the function.

    Fastrun is a method for offloading function execution either in the cloud or on a separate process.
    For more documentation on fastrun, visit https://fastmap.io/docs
    """

INIT_DOCSTRING = """\n
    :param secret: The API token generated on fastmap.io. Treat this like a password. Do not commit it to version control! If None, fastmap will run locally.
    :param verbosity: One of 'QUIET', 'NORMAL', or 'LOUD'. Default is 'NORMAL'.
    :param exec_policy: One of 'LOCAL', 'CLOUD', or 'ADAPTIVE'. Default is 'ADAPTIVE'.
    :param all_local_threads: By default, fastmap will utilize n - 1 available threads to allow other processes to remain performant. If set to True, fastmap may run marginally faster.
    :param confirm_charges: Manually confirm cloud charges.
    """

INITIAL_RUN_DUR = .1  # seconds
BATCH_DUR = 1.0  # seconds
PROCESS_OVERHEAD = 0.1  # seconds
CLOUD_OVERHEAD = 1.0  # seconds


def set_docstring(docstr, docstr_prefix=''):
    """ Add the given doc string to each function """
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


def fmt_bytes(num_bytes):
    """ Returns human-readable bytes string """
    if num_bytes > 1024**3:
        return "%.2fGB" % (num_bytes / 1024**3)
    if num_bytes > 1024**2:
        return "%.2fMB" % (num_bytes / 1024**2)
    if num_bytes > 1024:
        return "%.2fKB" % (num_bytes / 1024)
    return "%dB"


def encode(raw_data):
    return base64.b64encode(dill.dumps(raw_data)).decode()


def decode(encoded_data):
    return dill.loads(base64.b64decode(encoded_data.encode()))


def get_hash(obj):
    hasher = hashlib.sha256()
    hasher.update(obj)
    return hasher.hexdigest()


class ExecutionException(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """


class ExecPolicy():
    ADAPTIVE = "ADAPTIVE"
    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


class Verbosity():
    QUIET = "QUIET"
    NORMAL = "NORMAL"
    LOUD = "LOUD"


class Promise():
    pass


class BatchGenerator():
    """
    Takes an iterable and smartly turns it into batches of the correct size
    """
    def __init__(self, iterable, initial_runtime=None, batch_goal_time=1.0):
        self.iterable_len = -1
        self.iterable = iter(iterable)
        self.runtimes = []
        self.batch_goal_time = batch_goal_time
        self.batch_idx = -1
        self.element_cnt = 0
        if initial_runtime:
            self.runtimes.append(initial_runtime)

    def running_avg_runtime(self):
        try:
            return sum(self.runtimes[-5:]) / len(self.runtimes[-5:])
        except ZeroDivisionError:
            return self.batch_goal_time

    def total_avg_runtime(self):
        try:
            return sum(self.runtimes) / len(self.runtimes)
        except ZeroDivisionError:
            return None

    def num_batches_made(self):
        return self.batch_idx + 1

    def next(self, last_runtime=None):
        if last_runtime:
            self.runtimes.append(last_runtime)
        batch_size = int(self.batch_goal_time / self.running_avg_runtime())
        batch_seq = []
        for _ in range(batch_size):
            try:
                batch_seq.append(next(self.iterable))
            except StopIteration:
                break
        if not batch_seq:
            return None
        self.batch_idx += 1
        self.element_cnt += len(batch_seq)
        # sys.stdout.write("Generated batch %d. Elements in batch:%d Elements so far:%d\r" %
        #                  (self.batch_idx, len(batch_seq), self.element_cnt))
        # sys.stdout.flush()
        return self.batch_idx, batch_seq


def process_local(func, itdm, log):

    def logit(msg, *args):
        msg = msg % args
        if log.level <= 10:
            print("fastmap: " + msg)

    batch_tup = itdm.checkout()
    while batch_tup:
        batch_idx, batch_iter = batch_tup
        start = time.perf_counter()
        ret = list(map(func, batch_iter))
        total_proc_time = time.perf_counter() - start
        runtime = total_proc_time / len(ret)
        logit("Batch %d of size %d ran in %.2f (%.2e per element)",
              batch_idx, len(ret), total_proc_time, runtime)
        itdm.push_outbox(batch_idx, ret, runtime)
        batch_tup = itdm.checkout()


def _fastrun_local(queue, func, args, kwargs):
    queue.put(func(*args, **kwargs))


def get_additional_headers(secret):
    return {
        'Authorization': 'Bearer ' + secret,
        'content-encoding': 'gzip'
    }


def remote_connection_thread(thread_id, cloud_url_base, func_hash, encoded_func,
                             itdm, secret, log):

    def logit(msg, *args):
        msg = msg % args
        if log.level <= 10:
            print("fastmap: " + msg)

    # session = requests.Session()
    # session.headers['Authorization'] = "Bearer " + secret

    # resp = session.post(cloud_url_base + '/api/v1/init_map', json={'func': encoded_func})
    # if resp.status_code != 200:
    #     logit("Remote error %r" % pickle.loads(resp.content))
    # logit("In thread %d got resp to init_func %r" % (thread_id, resp.json()))

    batch_tup = itdm.checkout()
    additional_headers = get_additional_headers(secret)
    req_dict = {
        'func': encoded_func,
        'func_hash': func_hash,
    }


    while batch_tup:
        batch_idx, batch_iter = batch_tup
        start = time.perf_counter()
        req_dict['batch'] = batch_iter
        pickled = pickle.dumps(req_dict)
        encoded = base64.b64encode(pickled)
        payload = gzip.compress(encoded, compresslevel=1)
        encoding_time = time.perf_counter() - start
        logit("Starting request with %d elements. %s. %.2fB / element ",
              len(batch_iter), fmt_bytes(len(payload)), len(payload) / len(batch_iter))
        start = time.perf_counter()
        resp = requests.post(cloud_url_base + '/api/v1/execute',
                             data=payload, headers=additional_headers)
        req_time = time.perf_counter() - start
        if resp.status_code != 200:
            log.warning("Remote error %r" % pickle.loads(resp.content))
            break

        start = time.perf_counter()
        resp_dict = pickle.loads(base64.b64decode(resp.content))
        redecoding_time = time.perf_counter() - start

        results = resp_dict['results']
        remote_cid = resp.headers['X-Container-Id']
        remote_tid = resp.headers['X-Thread-Id']

        total_request = encoding_time + req_time + redecoding_time
        total_application = float(resp.headers['X-Total-Seconds'])
        total_processing = resp_dict['map_seconds']
        req_time_per_el = total_request / len(results)
        app_time_per_el = total_application / len(results)
        proc_time_per_el = total_processing / len(results)

        logit("Got %d results from %s/%s in thread %d",
              len(results), remote_cid, remote_tid, thread_id)
        logit("Batch processed for %.2f/%.2f/%.2f map/app/req (%.2e/%.2e/%.2e per element)",
              total_processing, total_application, total_request, proc_time_per_el,
              app_time_per_el, req_time_per_el)
        itdm.push_outbox(batch_idx,
                         results,
                         None,
                         vcpu_seconds=total_application)
        batch_tup = itdm.checkout()


class _RemoteProcessor(multiprocessing.Process):
    def __init__(self, func, itdm, parent):
        multiprocessing.Process.__init__(self)
        self.itdm = itdm
        self.func = func

        pickled_func = dill.dumps(func)
        self.func_hash = get_hash(pickled_func)
        self.encoded_func = pickled_func

        self.log = parent.log
        self.max_cloud_connections = parent.max_cloud_connections
        self.cloud_url_base = parent.cloud_url_base
        self.secret = parent.secret
        self.log.info("Started remote processor. Func payload is %dB", len(self.encoded_func))

    def run(self):
        threads = []
        self.log.debug("Opening %d remote connection(s)", self.max_cloud_connections)
        for thread_id in range(self.max_cloud_connections):
            self.log.debug("Creating remote thread %r", thread_id)
            thread_args = (thread_id, self.cloud_url_base, self.func_hash, self.encoded_func,
                           self.itdm, self.secret, self.log)
            thread = threading.Thread(target=remote_connection_thread, args=thread_args)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


class InterThreadDataManager():

    def __init__(self, iterable_len=None, max_inbox=None):
        manager = multiprocessing.Manager()
        self._lock = multiprocessing.Lock()
        self._inbox = manager.list()
        self._outbox = manager.list()
        self._runtimes = manager.list()
        # self._loans = manager.dict()

        self.state = manager.dict()
        self.state['inbox_done'] = False
        self.state['iterable_len'] = iterable_len
        self.state['max_inbox'] = max_inbox
        self.state['total_vcpu_seconds'] = 0.0

    def mark_inbox_done(self):
        with self._lock:
            assert not self.state['inbox_done']
            self.state['inbox_done'] = True

    def push_inbox(self, item_tup):
        while True:
            with self._lock:
                if self.state['max_inbox'] is None or len(self._inbox) < self.state['max_inbox']:
                    assert isinstance(item_tup, tuple) and len(item_tup) == 2
                    if self.state['inbox_done']:
                        raise AssertionError
                    self._inbox.append(item_tup)
                    return

    def checkout(self):
        while True:
            with self._lock:
                if self.state['inbox_done'] and not len(self._inbox):
                    return None
                if self._inbox:
                    item_tup = self._inbox.pop(0)
                    # self._loans[item_tup[0]] = item_tup[1]
                    return item_tup

    def push_outbox(self, item_idx, result, runtime, vcpu_seconds=None):
        with self._lock:
            self._outbox.append((item_idx, result))
            if runtime:
                self._runtimes.append(runtime)
            if vcpu_seconds:
                self.state['total_vcpu_seconds'] += vcpu_seconds
            # del self._loans[item_idx]

    def pop_outbox(self, processors):
        while True:
            with self._lock:
                if len(self._outbox):
                    return self._outbox.pop(0)
            if not any(p.is_alive() for p in processors):
                raise ExecutionException("Every process died. Check for errors in your code")
            time.sleep(.01)

    def reprocess_loans(self):
        with self._lock:
            self._inbox = sorted(self._loans.items()) + self._inbox
            # self._loans = {}


def fill_inbox(batch_generator, itdm):
    batch_tup = batch_generator.next()
    while batch_tup:
        itdm.push_inbox(batch_tup)

        last_runtimes = itdm._runtimes[-5:]
        try:
            last_runtime = sum(last_runtimes) / len(last_runtimes)
        except ZeroDivisionError:
            last_runtime = None
        batch_tup = batch_generator.next(last_runtime)
    itdm.mark_inbox_done()


class FastmapConfig():
    """
    The configuration object. Do not instantiate this directly.
    Instead, either:
    - use init to get a new FastmapConfig object
    - use global_init to allow fastmap to run without an init object.

    This object exposes one public method: fastmap.
    """

    def __init__(self, secret=None, verbosity=Verbosity.NORMAL,
                 exec_policy=ExecPolicy.ADAPTIVE, confirm_charges=False,
                 all_local_threads=False):
        self.exec_policy = exec_policy
        self._init_exec_policy(verbosity)
        self._init_logging(verbosity)
        self.confirm_charges = confirm_charges
        self.num_local_threads = os.cpu_count()
        self.cloud_url_base = 'https://fastmap.io'
        self.max_cloud_connections = 1

        if secret:
            self.secret = secret
        else:
            self.secret = None
            self.exec_policy = "LOCAL"
            self.log.warning("No secret provided. The exec_policy is now set to LOCAL")

        # try:
        #     self.num_local_threads = len(psutil.Process().cpu_affinity())
        # except AttributeError:
        #     self.num_local_threads = psutil.cpu_count()
        if not all_local_threads:
            self.num_local_threads -= 2

        if not confirm_charges and exec_policy != ExecPolicy.LOCAL:
            self.log.warning("The parameter 'confirm_charges' is False. Your vCPU-hour balance "
                             "will be automatically debited for use.")

        self.log.debug("Setup fastmap")
        self.log.debug(" verbosity: %s.", verbosity)
        self.log.debug(" exec_policy: %s", exec_policy)

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable):
        start_time = time.perf_counter()
        fastmapper = _FastMapper(self)

        if hasattr(iterable, '__len__'):
            total_unprocessed = len(iterable)
            total_processed = 0
            for batch in fastmapper.fastmap(func, iterable):
                for el in batch:
                    yield el
                total_processed += len(batch)
                percent = total_processed / total_unprocessed * 100
                graph_bar = '█' * int(percent / 2)
                sys.stdout.write('fastmap: \033[92m%s %.1f%%\r\033[0m' % (graph_bar, percent))
                sys.stdout.flush()
            print()
        else:
            total_processed = 0
            for batch in fastmapper.fastmap(func, iterable):
                for el in batch:
                    yield el
                total_processed += len(batch)
                sys.stdout.write('fastmap: processed %d\r' % total_processed)
                sys.stdout.flush()
            print()
        total_duration = time.perf_counter() - start_time
        network_duration = -1

        if fastmapper.avg_runtime:
            time_saved = fastmapper.avg_runtime * total_processed - total_duration
            if time_saved > 3600:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f hours",
                              total_processed, total_duration, time_saved / 3600)
            if time_saved > 60:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f minutes",
                              total_processed, total_duration, time_saved / 60)
            elif time_saved > 0.01:
                self.log.info("Processed %d elements in %.2fs. "
                              "You saved %.2f seconds",
                              total_processed, total_duration, time_saved)
            elif abs(time_saved) < 0.01:
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran at about the same speed as the builtin map.",
                              total_processed, total_duration)
            elif self.exec_policy == "LOCAL":
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran slower than the map builtin by %.2f seconds (estimate). "
                              "Consider not using fastmap here.",
                              total_processed, total_duration, time_saved * -1)
            else:
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran slower than the map builtin by %.2f seconds (estimate). "
                              "Network transfer accounts for %.2f of this duration. "
                              "Consider upgrading your connection, reducing your data size, "
                              "or using exec_policy LOCAL or ADAPTIVE.",
                              total_processed, total_duration, time_saved * -1, network_duration)
        else:
            self.log.info("Fastmap processing done.")

        if fastmapper.total_vcpu_seconds:
            self.log.debug("Used %.4f vCPU-hours.", fastmapper.total_vcpu_seconds/60/60)

    @set_docstring(FASTRUN_DOCSTRING)
    def fastrun(self, func, *args, **kwargs):
        start_time = time.perf_counter()
        if self.exec_policy == "LOCAL":
            queue = multiprocessing.Queue()
            proc = multiprocessing.Process(_fastrun_local, (queue, func, args, kwargs))
            proc.start()
            proc.join()
            results = queue.get()
            runtime = time.perf_counter() - start_time
            self.log.info("Fastrun processing done in %.2f.", runtime)
            return results

        req_dict = {
            "func": dill.dumps(func),
            "args": args,
            "kwargs": kwargs
            }
        pickled = pickle.dumps(req_dict)
        encoded = base64.b64encode(pickled)
        payload = gzip.compress(encoded, compresslevel=1)

        resp = requests.post(self.cloud_url_base + "/api/v1/run", data=payload,
                             headers=get_additional_headers(self.secret))
        resp_dict = pickle.loads(resp.content)
        if resp.status_code != 200:
            raise ExecutionException("Remote error %r" % resp_dict)

        runtime = time.perf_counter() - start_time
        self.log.info("Fastrun processing done in %.2f.", runtime)
        self.log.debug("Used %.4f vCPU-hours", resp.headers['vcpu_seconds']/60/60)
        return resp_dict['result']


    def _init_exec_policy(self, exec_policy):
        pass  # TODO
        # if exec_policy == "LOCAL":
        #     self.runners = (_local_run,)
        # elif exec_policy == "CLOUD":
        #     self.runners = (_cloud_run,)
        # elif exec_policy == "ADAPTIVE":
        #     self.runners = (_local_run, _cloud_run)
        # else:
        #     raise AssertionError(f"Unknown value for exec_policy '{exec_policy}'")
        # self.exec_policy = exec_policy

    def _init_logging(self, verbosity):
        self.log = logging.getLogger('fastmap')

        if verbosity == Verbosity.QUIET:
            self.log.setLevel(logging.CRITICAL)
        elif verbosity == Verbosity.NORMAL:
            self.log.setLevel(logging.INFO)
        elif verbosity == Verbosity.LOUD:
            self.log.setLevel(logging.DEBUG)
        else:
            raise AssertionError(f"Unknown value for verbosity '{verbosity}'")
        self.verbosity = verbosity

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s: %(message)s')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)


class _FastMapper():
    """
    Wrapper for running fastmap. Maintains state variables including
    iterable_len and avg_runtime.
    """
    def __init__(self, config):
        self.config = config
        self.log = config.log
        self.iterable_len = None
        self.avg_runtime = None

    def _estimated_runtime(self):
        if self.avg_runtime and self.iterable_len:
            return self.avg_runtime * self.iterable_len
        return None

    def _get_processors(self, func):
        if self.config.exec_policy == 'CLOUD':
            max_batches_in_queue = self.config.max_cloud_connections  # TODO make this smarter
        elif self.config.exec_policy == 'LOCAL':
            max_batches_in_queue = self.config.num_local_threads
        else:
            max_batches_in_queue = self.config.num_local_threads + self.config.max_cloud_connections

        itdm = InterThreadDataManager(self.iterable_len, max_batches_in_queue)

        processors = []
        if self.config.exec_policy != 'CLOUD':
            for _ in range(self.config.num_local_threads):
                process_args = (func, itdm, self.log)
                local_process = multiprocessing.Process(target=process_local, args=process_args)
                local_process.start()
                processors.append(local_process)
            self.log.debug("Launching %d local processes", self.config.num_local_threads)
        if self.config.exec_policy != 'LOCAL':
            remote_process = _RemoteProcessor(func, itdm, self.config)
            remote_process.start()
            processors.append(remote_process)
        return processors, itdm

    def fastmap(self, func, iterable):
        self.log.debug(f"Running fastmap on {func.__name__}")

        self.avg_runtime = None
        if hasattr(iterable, "__len__"):
            self.iterable_len = len(iterable)
        else:
            self.iterable_len = None

        iterable = iter(iterable)

        start_timing_loop = time.perf_counter()
        ret = []
        while time.perf_counter() - start_timing_loop < INITIAL_RUN_DUR:
            try:
                item = next(iterable)
            except StopIteration:
                self.avg_runtime = (time.perf_counter() - start_timing_loop) / len(ret)
                self.log.debug(f"Fastmap finished faster than {INITIAL_RUN_DUR} seconds")
                yield ret
                return
            ret.append(func(item))
        self.avg_runtime = (time.perf_counter() - start_timing_loop) / len(ret)

        total_yielded = len(ret)
        yield ret

        # if self.iterable_len:
        #     total_process_overhead = self.config.num_local_threads * PROCESS_OVERHEAD
        #     total_process_time_estimate = self.estimated_run_time() / self.config.num_local_threads
        #     if self.estimated_run_time() > total_process_overhead + total_process_time_estimate:
        #         iterable = list(iterable)
        #         yield list(map(func, iterable))

        #     if estimated_run_time < CLOUD_OVERHEAD:
        #         with multiprocessing.Pool(self.config.num_local_threads) as pool:
        #             yield pool.map(func, iterable)
        #         return

        processors, itdm = self._get_processors(func)
        batch_generator = BatchGenerator(iterable, self.avg_runtime)

        fill_inbox_thread = threading.Thread(target=fill_inbox,
                                             args=(batch_generator, itdm))
        fill_inbox_thread.start()

        cur_idx = 0
        staging = {}
        while not itdm.state['inbox_done'] or cur_idx < batch_generator.num_batches_made():
            result_idx, result_list = itdm.pop_outbox(processors)
            staging[result_idx] = result_list
            while cur_idx in staging:
                to_yield = staging.pop(cur_idx)
                yield to_yield
                total_yielded += len(to_yield)
                cur_idx += 1

        fill_inbox_thread.join()

        for processor in processors:
            processor.join()

        self.avg_runtime = batch_generator.total_avg_runtime()
        self.total_vcpu_seconds = itdm.state['total_vcpu_seconds']
        self.iterable_len = batch_generator.element_cnt
