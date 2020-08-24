import base64
import gzip
import hashlib
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

FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the results.
    Depending on prior configuration, fastmap will run either locally via multiprocessing,
    in the cloud on the fastmap.io servers, or adaptively on both.

    :param func: Function to map with. Must be stateless and not use the network or the filesystem.
    :param iterable: Iterable to map over.

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in most circumstances.
    For more documentation on fastmap, visit https://fastmap.io/docs
    """

FASTRUN_DOCSTRING = """
    Start a function and returns a Promise object.
    Depending on prior configuration, fastmap will run either locally via a separate process,
    in the cloud on the fastmap.io servers, or adaptively on both.

    :param func: Function to map with. Must be stateless and not use the network or the filesystem.
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

dill.settings['recurse'] = True



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
    return "%dB" % num_bytes

def fmt_time(num_seconds):
    hours, remainder = divmod(num_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))
    if minutes > 0:
        return '{:02}:{:02}'.format(int(minutes), int(seconds))
    return '{}s'.format(int(seconds))


def encode(raw_data):
    return base64.b64encode(dill.dumps(raw_data)).decode()


def decode(encoded_data):
    return dill.loads(base64.b64decode(encoded_data.encode()))


def get_hash(obj):
    hasher = hashlib.sha256()
    hasher.update(obj)
    return hasher.hexdigest()


class ExecutionError(Exception):
    """
    Thrown when something goes wrong running the user's code on the
    cloud or on separate processes.
    """

class RemoteError(Exception):
    """
    Thrown when the remote connection results in a non-200
    """

class ExecPolicy():
    ADAPTIVE = "ADAPTIVE"
    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


class Verbosity():
    QUIET = "QUIET"
    NORMAL = "NORMAL"
    LOUD = "LOUD"


def process_local(func, itdm, log, error_queue):
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
        error_queue.put((multiprocessing.current_process().name, repr(e)))


def _fastrun_local(queue, func, args, kwargs):
    queue.put(func(*args, **kwargs))


def get_headers(secret):
    """Get headers for communicating with the fastmap.io cloud service"""
    return {
        'Authorization': 'Bearer ' + secret,
        'Content-Encoding': 'gzip',
        'X-Python-Version': sys.version.replace('\n', ''),
    }




def process_one_batch_remotely(itdm, batch_tup, url, req_dict, headers, log):
    start_req_time = time.perf_counter()

    batch_idx, batch = batch_tup
    req_dict['batch'] = batch
    pickled = pickle.dumps(req_dict)
    encoded = base64.b64encode(pickled)
    payload = gzip.compress(encoded, compresslevel=1)
    log.debug("Making cloud request with %d elements. %s. %.2fB / element ",
              len(batch), fmt_bytes(len(payload)), len(payload) / len(batch))
    try:
        resp = requests.post(url, data=payload, headers=headers)
    except requests.exceptions.ConnectionError:
        raise RemoteError("Fastmap could not connect to the cloud server. Check your connection.")
    if resp.status_code == 200:
        resp_dict = pickle.loads(base64.b64decode(resp.content))
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
        log.debug("Batch processed for %.2f/%.2f/%.2f map/app/req (%.2e/%.2e/%.2e per element)",
                  total_mapping, total_application, total_request, map_time_per_el,
                  app_time_per_el, req_time_per_el)
        itdm.push_outbox(batch_idx,
                         results,
                         None,
                         vcpu_seconds=total_application,
                         network_seconds=total_request-total_application)
        return

    if resp.status_code == 401:
        raise RemoteError("Unauthorized. Check your API token.")
    if resp.status_code == 402:
        raise RemoteError("You do not have enough vCPU-hour credits to make this request. "
                    "You have %.2f available but the remote process is requiring %.2f. "
                    "Buy more vCPU-hours at https://fastmap.io", -1, -1)
    if resp.status_code == 400:
        raise RemoteError("Your code could not be processed remotely. Check your code for errors.")
    try:
        resp_out = pickle.loads(resp.content)
    except pickle.UnpicklingError:
        resp_out = resp.content
    raise RemoteError("Unexpected remote error %d %r", resp.status_code, resp_out)



def remote_connection_thread(thread_id, cloud_url_base, func_hash, encoded_func,
                             itdm, secret, log, error_queue):

    batch_tup = itdm.checkout()
    headers = get_headers(secret)
    req_dict = {
        'func': encoded_func,
        'func_hash': func_hash,
    }
    url = cloud_url_base + '/api/v1/execute'

    while batch_tup:
        try:
            process_one_batch_remotely(itdm, batch_tup, url, req_dict, headers, log)
        except Exception as e:
            proc_name = multiprocessing.current_process().name
            thread_id = threading.get_ident()
            error_queue.put(("%s: thread:%d" % (proc_name, thread_id), repr(e)))
            break
        batch_tup = itdm.checkout()


class _RemoteProcessor(multiprocessing.Process):
    def __init__(self, func, itdm, config, error_queue):
        multiprocessing.Process.__init__(self)
        self.itdm = itdm
        self.func = func
        self.error_queue = error_queue

        pickled_func = dill.dumps(func)
        self.func_hash = get_hash(pickled_func)
        self.encoded_func = pickled_func

        self.log = config.log
        self.max_cloud_connections = config.max_cloud_connections
        self.cloud_url_base = config.cloud_url_base
        self.secret = config.secret
        self.log.info("Initialized remote processor. Func payload is %dB", len(self.encoded_func))

    def start(self):
        # TODO did I want to use this for something? It does trigger and does so before .run
        super(_RemoteProcessor, self).start()

    def run(self):
        threads = []

        batch_tup = self.itdm.checkout()
        headers = get_headers(self.secret)
        req_dict = {
            'func': self.encoded_func,
            'func_hash': self.func_hash,
        }
        url = self.cloud_url_base + '/api/v1/execute'

        try:
            process_one_batch_remotely(self.itdm, batch_tup, url, req_dict, headers, self.log)
        except Exception as e:
            self.error_queue.put((multiprocessing.current_process().name, repr(e)))
            return

        self.log.debug("Opening %d remote connection(s)", self.max_cloud_connections)
        for thread_id in range(self.max_cloud_connections):
            self.log.debug("Creating remote thread %r", thread_id)
            thread_args = (thread_id, self.cloud_url_base, self.func_hash, self.encoded_func,
                           self.itdm, self.secret, self.log, self.error_queue)
            thread = threading.Thread(target=remote_connection_thread, args=thread_args)
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
        self._outbox = manager.list()
        self._runtimes = manager.list()
        # self._loans = manager.dict()

        self._runtimes.append(first_runtime)

        self.state = manager.dict()
        self.state['inbox_capped'] = False
        self.state['num_batches_put_in_inbox'] = None
        self.state['total_vcpu_seconds'] = 0.0
        self.state['total_network_seconds'] = 0.0

        self.batch_goal_time = 1.0

    def has_more_batches(self, cur_idx):
        if not self.state['inbox_capped']:
            return True
        return cur_idx < self.state['num_batches_put_in_inbox']

    def get_total_vcpu_seconds(self):
        return self.state['total_vcpu_seconds']

    def get_total_network_seconds(self):
        return self.state['total_network_seconds']

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

    def mark_inbox_capped(self, cnt):
        with self._lock:
            assert not self.state['inbox_capped']
            self.state['inbox_capped'] = True
            self.state['num_batches_put_in_inbox'] = cnt
            # self._inbox.close()

    def push_inbox(self, item):
        with self._lock:
            assert not self.state['inbox_capped']
            self._inbox.put(item)

    def _running_avg_runtime(self):
        last_five = self._runtimes[-5:]
        return sum(last_five) / len(last_five)

    def total_avg_runtime(self):
        try:
            return sum(self._runtimes) / len(self._runtimes)
        except ZeroDivisionError:
            return None

    def checkout(self):
        while True:
            if self.state['inbox_capped']:
                try:
                    return self._inbox.get_nowait()
                except multiprocessing.queues.Empty:
                    return None
            try:
                return self._inbox.get(timeout=.01)
            except multiprocessing.queues.Empty:
                pass

    def push_outbox(self, item_idx, result, runtime, vcpu_seconds=None, network_seconds=None):
        with self._lock:
            self._outbox.append((item_idx, result))
            if runtime:
                self._runtimes.append(runtime)
            if vcpu_seconds:
                self.state['total_vcpu_seconds'] += vcpu_seconds
            if network_seconds:
                self.state['total_network_seconds'] += network_seconds
            # del self._loans[item_idx]

    def pop_outbox(self, processors):
        while True:
            with self._lock:
                if len(self._outbox):
                    # print("popping outbox in InterThreadDataManager. Length of outbox", len(self._outbox))
                    return self._outbox.pop(0)
            if not any(p.is_alive() for p in processors):
                raise ExecutionError("Every execution process died.")
            time.sleep(.01)

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


class FillInboxWithIter(_FillInbox):
    def run(self):
        """
        Adapted from answer found here:
        https://stackoverflow.com/questions/8290397/how-to-split-an-iterable-in-constant-size-chunks
        Validated that this works well as an iterable (e.g. will yield JIT for slow generators)
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
        self.log.debug("Finished adding iterator to the task queue. %d batch(es). %d elements",
                       batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped(batch_cnt)


class FillInboxWithList(_FillInbox):
    def run(self):
        batch_len = self.batch_len()

        iter_len = len(self.iterable)
        batch_cnt = 0
        element_cnt = 0
        for idx in range(0, iter_len, batch_len):
            batch = self.iterable[idx:min(idx + batch_len, iter_len)]
            self.itdm.push_inbox((batch_cnt, batch))
            batch_cnt += 1
            element_cnt += len(batch)
            if self.do_die:
                return
        self.log.debug("Finished adding list to the task queue. %d batch(es). %d elements",
                       batch_cnt, element_cnt)
        self.itdm.mark_inbox_capped(batch_cnt)


class FastmapLogger():
    """
    This exists because it is difficult to pass python's native logger between processes and was
    requiring a lot of weird workarounds
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

    def __init__(self, secret=None, verbosity=Verbosity.NORMAL,
                 exec_policy=ExecPolicy.ADAPTIVE, confirm_charges=False,
                 all_local_threads=False):
        self._init_exec_policy(exec_policy)
        self._init_logging(verbosity)
        self.confirm_charges = confirm_charges
        self.num_local_threads = os.cpu_count()
        self.cloud_url_base = 'https://fastmap.io'
        self.max_cloud_connections = 1

        if secret:
            self.secret = secret
        else:
            self.secret = None
            if self.exec_policy != ExecPolicy.LOCAL:
                self.exec_policy = ExecPolicy.LOCAL
                self.log.warning("No secret provided. The exec_policy is now set to LOCAL")

        # try:
        #     self.num_local_threads = len(psutil.Process().cpu_affinity())
        # except AttributeError:
        #     self.num_local_threads = psutil.cpu_count()
        if not all_local_threads:
            self.num_local_threads -= 2

        if not confirm_charges and self.exec_policy != ExecPolicy.LOCAL:
            self.log.warning("The parameter 'confirm_charges' is False. Your vCPU-hour balance "
                             "will be automatically debited for use.")

        self.log.debug("Setup fastmap")
        self.log.debug(" verbosity: %s.", verbosity)
        self.log.debug(" exec_policy: %s", exec_policy)

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable):
        start_time = time.perf_counter()
        fastmapper = FastMapper(self)
        is_list = hasattr(iterable, '__len__')

        try:
            if self.verbosity == Verbosity.QUIET:
                for batch in fastmapper.fastmap(func, iterable, is_list=is_list):
                    for el in batch:
                        yield el
                return

            num_processed = 0
            if is_list:
                iter_len = len(iterable)
                for batch in fastmapper.fastmap(func, iterable, is_list=True):
                    for el in batch:
                        yield el
                    num_processed += len(batch)
                    percent = num_processed / iter_len * 100
                    time_remaining = (time.perf_counter() - start_time) * (iter_len - num_processed) / num_processed
                    graph_bar = '█' * int(percent / 2)
                    sys.stdout.write("fastmap: \033[92m%s %.1f%% (%s remaining)\r\033[0m" %
                                     (graph_bar, percent, fmt_time(time_remaining)))
                    sys.stdout.flush()
            else:
                for batch in fastmapper.fastmap(func, iterable, is_list=False):
                    for el in batch:
                        yield el
                    num_processed += len(batch)
                    sys.stdout.write("fastmap: Processed %d\r" % num_processed)
                    sys.stdout.flush()
        except Exception as e:
            try:
                while True:
                    process_error = fastmapper.error_queue.get_nowait()
                    self.log.error(" Process error [%s]: %s", process_error[0], process_error[1])
            except multiprocessing.queues.Empty:
                pass
            fastmapper.cleanup()
            raise e

        total_duration = time.perf_counter() - start_time
        self._log_final_stats(fastmapper.avg_runtime, num_processed, total_duration,
                              fastmapper.total_network_seconds, fastmapper.total_vcpu_seconds)

    def _log_final_stats(self, avg_runtime, num_processed, total_duration,
                         total_network_seconds, total_vcpu_seconds):

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
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran at about the same speed as the builtin map.",
                              num_processed, total_duration)
            elif self.exec_policy == ExecPolicy.LOCAL:
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran slower than the map builtin by %.2f seconds (estimate). "
                              "Consider not using fastmap here.",
                              num_processed, total_duration, time_saved * -1)
            else:
                self.log.info("Processed %d elements in %.2fs. "
                              "This ran slower than the map builtin by %.2f seconds (estimate). "
                              "Network transfer accounts for %.2fs of this duration. "
                              "Consider upgrading your connection, reducing your data size, "
                              "or using exec_policy LOCAL or ADAPTIVE.",
                              num_processed, total_duration, time_saved * -1,
                              total_network_seconds)

        if total_vcpu_seconds:
            self.log.info("Used %.4f vCPU-hours.", total_vcpu_seconds/60/60)

    # @set_docstring(FASTRUN_DOCSTRING)
    # def fastrun(self, func, *args, **kwargs):
    #     start_time = time.perf_counter()
    #     if self.exec_policy == "LOCAL":
    #         queue = multiprocessing.Queue()
    #         proc = multiprocessing.Process(_fastrun_local, (queue, func, args, kwargs))
    #         proc.start()
    #         proc.join()
    #         results = queue.get()
    #         runtime = time.perf_counter() - start_time
    #         self.log.info("Fastrun processing done in %.2f.", runtime)
    #         return results

    #     req_dict = {
    #         "func": dill.dumps(func),
    #         "args": args,
    #         "kwargs": kwargs
    #         }
    #     pickled = pickle.dumps(req_dict)
    #     encoded = base64.b64encode(pickled)
    #     payload = gzip.compress(encoded, compresslevel=1)

    #     resp = requests.post(self.cloud_url_base + "/api/v1/run", data=payload,
    #                          headers=get_headers(self.secret))
    #     resp_dict = pickle.loads(resp.content)
    #     if resp.status_code == 200:
    #         runtime = time.perf_counter() - start_time
    #         self.log.info("Fastrun processing done in %.2f.", runtime)
    #         self.log.debug("Used %.4f vCPU-hours", resp.headers['vcpu_seconds']/60/60)
    #         return resp_dict['result']
    #     if resp.status_code == 401:
    #         raise UnauthorizedException("Bad client token")
    #     else:
    #         raise ExecutionError("Remote error %r" % resp_dict)

    def _init_exec_policy(self, exec_policy):
        if exec_policy not in (ExecPolicy.ADAPTIVE, ExecPolicy.CLOUD, ExecPolicy.LOCAL):
            raise AssertionError(f"Unknown value for exec_policy '{exec_policy}'")
        self.exec_policy = exec_policy

    def _init_logging(self, verbosity):
        self.log = FastmapLogger(verbosity)
        self.verbosity = verbosity
        # self.log = logging.getLogger('fastmap')

        # if verbosity == Verbosity.QUIET:
        #     self.log.setLevel(logging.CRITICAL)
        # elif verbosity == Verbosity.NORMAL:
        #     self.log.setLevel(logging.INFO)
        # elif verbosity == Verbosity.LOUD:
        #     self.log.setLevel(logging.DEBUG)
        # else:
        #     raise AssertionError(f"Unknown value for verbosity '{verbosity}'")
        # self.verbosity = verbosity

        # ch = logging.StreamHandler()
        # formatter = logging.Formatter('%(name)s: %(message)s')
        # ch.setFormatter(formatter)
        # self.log.addHandler(ch)


class FastMapper():
    """
    Wrapper for running fastmap. Maintains state variables including
    iter_len and avg_runtime.
    """
    INITIAL_RUN_DUR = 0.1  # seconds
    PROCESS_OVERHEAD = 0.1  # seconds

    def __init__(self, config):
        self.config = config
        self.log = config.log
        self.avg_runtime = None
        self.total_vcpu_seconds = 0
        self.total_network_seconds = 0
        self.error_queue = multiprocessing.Queue()

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


    def _get_vcpu_hours_estimate(self, iter_len):
        # TODO take local processing into account
        return self.avg_runtime * iter_len / 60.0 / 60.0

    def _confirm_charges(self, iterable):
        if not self.config.confirm_charges:
            return True
        while True:
            if hasattr(iterable, "__len__"):
                user_input_question = "Estimated cloud usage of %.4f vCPU-hours." \
                                      " Continue?" % self._get_vcpu_hours_estimate(len(iterable))
            else:
                user_input_question = "Unable to estimate cloud usage because " \
                                      "sequence was an iterable. Continue anyway?"
            user_input = self.log.input("%s (y/n) " % user_input_question)
            if user_input.lower() == 'y':
                return True
            elif user_input.lower() == 'n':
                if self.config.exec_policy == ExecPolicy.ADAPTIVE:
                    self.log.info("Cloud operation cancelled. Continuing processing locally...")
                return False
            else:
                self.log.warning("Unrecognized input of %r. Please input 'y' or 'n'" % user_input)


    def _get_processors(self, func, iterable):
        if self.config.exec_policy == ExecPolicy.CLOUD:
            max_batches_in_queue = self.config.max_cloud_connections  # TODO make this smarter
        elif self.config.exec_policy == ExecPolicy.LOCAL:
            max_batches_in_queue = self.config.num_local_threads
        else:
            max_batches_in_queue = self.config.num_local_threads + self.config.max_cloud_connections

        itdm = InterThreadDataManager(iterable, self.avg_runtime, max_batches_in_queue)

        processors = []
        if self.config.exec_policy != ExecPolicy.CLOUD:
            for _ in range(self.config.num_local_threads):
                process_args = (func, itdm, self.log, self.error_queue)
                local_process = multiprocessing.Process(target=process_local, args=process_args)
                local_process.start()
                processors.append(local_process)
            self.log.debug("Launching %d local processes", self.config.num_local_threads)
        if self.config.exec_policy != ExecPolicy.LOCAL:
            if self._confirm_charges(iterable):
                remote_process = _RemoteProcessor(func, itdm, self.config, self.error_queue)
                remote_process.start()
                processors.append(remote_process)

        if not processors or not any(p.is_alive() for p in processors):
            raise ExecutionError("No execution processes started. " \
                                 "Try modifying your fastmap configuration.")

        return processors, itdm

    def initial_run(self, func, iterable):
        iterable = iter(iterable)
        ret = []
        start_time = time.perf_counter()
        end_loop_at = start_time + self.INITIAL_RUN_DUR
        while True:
            try:
                item = next(iterable)
            except StopIteration:
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


    def fastmap(self, func, iterable, is_list=False):
        if is_list:
            iter_len = len(iterable)
            self.log.debug("Applying %r to %d items and yielding the results.",
                           func.__name__, iter_len)
        else:
            self.log.debug("Appling %r to an iterator and yielding the results.", func.__name__)

        initial_batch, has_more = self.initial_run(func, iterable)
        yield initial_batch
        if not has_more:
            return

        if is_list:
            iterable = iterable[len(initial_batch):]

        if is_list and self.config.exec_policy != ExecPolicy.CLOUD:
            total_process_overhead = self.config.num_local_threads * self.PROCESS_OVERHEAD
            estimated_runtime = self.avg_runtime * len(iterable)
            total_process_time_estimate = estimated_runtime / self.config.num_local_threads
            if estimated_runtime < total_process_overhead + total_process_time_estimate:
                self.log.debug("Running single threaded due to short expected runtime")
                yield list(map(func, iterable))
                return

        self.processors, self.itdm = self._get_processors(func, iterable)

        if is_list:
            self.fill_inbox_thread = FillInboxWithList(iterable, self.itdm, self.log, self.avg_runtime)
        else:
            self.fill_inbox_thread = FillInboxWithIter(iterable, self.itdm, self.log, self.avg_runtime)
        self.fill_inbox_thread.start()

        cur_idx = 0
        staging = {}
        while self.itdm.has_more_batches(cur_idx):
            result_idx, result_list = self.itdm.pop_outbox(self.processors)
            staging[result_idx] = result_list
            while cur_idx in staging:
                to_yield = staging.pop(cur_idx)
                yield to_yield
                cur_idx += 1

        self.log.debug("Cleaning up threads and processes...")
        self.fill_inbox_thread.join()
        for processor in self.processors:
            processor.join()

        self.total_vcpu_seconds = self.itdm.get_total_vcpu_seconds()
        self.avg_runtime = self.itdm.total_avg_runtime()
        self.total_network_seconds = self.itdm.get_total_network_seconds()
