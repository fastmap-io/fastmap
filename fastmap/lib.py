import base64
import logging
import os
import multiprocessing
import time
import sys


# import capnp
# import fastmap_capnp
import dill
# import psutil
from requests_futures.sessions import FuturesSession
import requests

_transfer_durs = {}
dill.settings['recurse'] = True


FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the results.

    :param func: Function to map with. Must have no side effects.
    :param iterable: Iterable to map over.

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in almost all circumstances.
    For more documentation on fastmap, visit https://fastmap.io/
    """

INIT_DOCSTRING = """\n
    :param secret: The API token generated on fastmap.io. Keep this secure and do not commit it to version control! If None, fastmap will run locally.
    :param verbosity: One of 'QUIET', 'NORMAL', or 'LOUD'. Default is 'NORMAL'.
    :param exec_policy: One of 'LOCAL', 'CLOUD', or 'ADAPTIVE'. Default is 'ADAPTIVE'.
    :param all_local_cpus: By default, fastmap will utilize n - 1 available threads to allow other processes to remain performant. If set to True, fastmap will run marginally faster.
    :param confirm_charges: Manually confirm cloud charges.
    """

INITIAL_RUN_DUR = 1.0  # seconds
BATCH_DUR = 5.0 # seconds
PROCESS_OVERHEAD = 0.1  # seconds


def set_docstring(docstr, docstr_prefix=''):
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


def encode(raw_data):
    return base64.b64encode(dill.dumps(raw_data)).decode()


def decode(encoded_data):
    return dill.loads(base64.b64decode(encoded_data.encode()))


class ExecPolicy(object):
    ADAPTIVE = "ADAPTIVE"
    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


class Verbosity(object):
    QUIET = "QUIET"
    NORMAL = "NORMAL"
    LOUD = "LOUD"


from itertools import islice, chain

# def gen_batches(iterable, size):
#     sourceiter = iter(iterable)
#     while True:
#         batchiter = islice(sourceiter, size)
#         yield chain([next(batchiter)], batchiter)

def gen_batches(iterable, batch_size):
    iterator = iter(iterable)
    for first in iterator:
        yield list(chain([first], islice(iterator, batch_size - 1)))

# def gen_batches(iterable, batch_size):
#     "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
#     args = [iter(iterable)] * batch_size
#     return zip(*args)

# def gen_batches(data, batch_size):
#     num_batches = (len(data) // batch_size)
#     print("len(data)", len(data))
#     print("batch_size", batch_size)
#     print("num_batches", num_batches)
#     for i in range(num_batches):
#         # percent = (i / num_batches) * 100
#         # sys.stdout.write('%s %.1f%%\r' % ('█' * int(percent / 2), percent))
#         # sys.stdout.flush()
#         yield data[i*batch_size:(i+1)*batch_size]
#     # sys.stdout.write('%s %.1f%%\n' % ('█' * 50, 100))
#     # sys.stdout.flush()


def process_local(func, batches, results):
    batch = batches.get()
    while batch:
        results.put(list(map(func, batch)))
        batch = batches.get()


class _RemoteProcessor(multiprocessing.Process):
    def __init__(self, func, batches, results, parent):
        multiprocessing.Process.__init__(self)
        self.batches = batches
        self.results = results
        self.encoded_func = encode(func)
        self.log = parent.log
        # self.connections = []
        self.cloud_url_base = parent.cloud_url_base
        self.session = parent.session
        # self.futures_session = FuturesSession()
        self.log.info("Started remote processor. Function payload is %d bytes", len(self.encoded_func))

    def run(self):
        batch = self.batches.get()
        while batch:
            self._run_one(batch)
            batch = self.batches.get()

    def _run_one(self, batch):
        payload = {'func': self.encoded_func, 'batch': encode(batch)}
        # promise = self.futures_session.post(self.cloud_url_base + '/api/v1/execute', json=payload)
        resp = self.session.post(self.cloud_url_base + '/api/v1/execute', json=payload)
        if resp.status_code != 200:
            self.log.error("Remote error %r", resp.text)
        resp_json = resp.json()
        if resp_json['status'] != "OK":
            self.log.error("Status not OK %r", resp_json)
        # print(resp_json)
        self.results.put(decode(resp_json['results']))


    # def run(self):
    #     payload = {'func': self.encoded_func}

    #     resp = self.futures_session.post(self.cloud_url_base + '/api/v1/init_map', json=payload).result()

    #     if resp.status_code != 200:
    #         self.log.error("Remote error %r", resp.text)

    #     resp_json = resp.json()
    #     if resp_json['status'] != "OK":
    #         self.log.error("Status not OK %r", resp_json)

    #     batch = self.batches.get()
    #     while batch:
    #         # TODO this is still sequential
    #         resp = self.futures_session.post(self.cloud_url_base + '/api/v1/execute_data', json=payload).result()
    #         resp_json = resp.json()
    #         self.results.put(resp_json['results'])


class FastmapConfig(object):
    """
    The configuration object. Do not instantiate this directly.
    Instead, either:
    - use init to get a new FastmapConfig object
    - use global_init to allow fastmap to run without an init object.

    This object exposes one public method: fastmap.
    """

    def __init__(self, secret=None, verbosity=Verbosity.NORMAL,
                 exec_policy=ExecPolicy.ADAPTIVE, confirm_charges=False,
                 all_local_cpus=False):
        self.exec_policy = exec_policy
        self._init_exec_policy(verbosity)
        self._init_logging(verbosity)
        self.confirm_charges = confirm_charges
        self.num_threads = os.cpu_count()
        self.cloud_url_base = 'https://fastmap.io'
        self.avg_local_run_time = None

        if secret:
            self.session = requests.Session()
            self.session.headers['Authorization'] = "Bearer " + secret
        else:
            self.session = None
            self.exec_policy = "LOCAL"
            self.log.warning("No secret provided. The exec_policy is now set to LOCAL")

        # try:
        #     self.num_threads = len(psutil.Process().cpu_affinity())
        # except AttributeError:
        #     self.num_threads = psutil.cpu_count()
        if not all_local_cpus:
            self.num_threads -= 2

        if not confirm_charges and exec_policy != ExecPolicy.LOCAL:
            self.log.warning("The parameter 'confirm_charges' is False. Your vCPU-hour balance "
                             "will be automatically debited if used")

        self.log.info("Setup fastmap")
        self.log.info(f" verbosity: {verbosity}.")
        self.log.info(f" exec_policy: {exec_policy}")

        # self.client = capnp.TwoPartyClient(Defaults.CLOUD_URL)
        # self.fastmap_wrapper = self.client.bootstrap().cast_as(fastmap_capnp.Fastmap)

    def get_processors(self, func):
        batches = multiprocessing.Queue()
        results = multiprocessing.Queue()

        processors = []
        if self.exec_policy != 'CLOUD':
            for _ in range(self.num_threads):
                local_process = multiprocessing.Process(target=process_local, args=(func, batches, results))
                local_process.start()
                processors.append(local_process)
        if self.exec_policy != 'LOCAL':
            remote_process = _RemoteProcessor(func, batches, results, self)
            remote_process.start()
            processors.append(remote_process)
        return processors, batches, results

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable):
        iterable = list(iterable)
        orig_len = len(iterable)
        start_time = time.perf_counter()
        ret, avg_local_run_time = self._fastmap(func, iterable)
        duration = time.perf_counter() - start_time
        self.log.info("Duration: %.2f", duration)
        if avg_local_run_time:
            time_saved = avg_local_run_time * orig_len - duration
            if time_saved > 60:
                self.log.info("Fastmap processing done. You saved %.2f minutes", time_saved/60)
            elif time_saved > 0:
                self.log.info("Fastmap processing done. You saved %.2f seconds", time_saved)
            else:
                self.log.info("Fastmap processing done. You would have been better not using fastmap %.2f", time_saved)

        return ret

    def _fastmap(self, func, iterable):
        # start = time.perf_counter()
        orig_len = len(iterable)
        self.log.info(f"Running fastmap on {func.__name__} with {orig_len} elements")

        if not iterable:
            return [], None
        # print("a", time.perf_counter() - start)

        # pickled_func = dill.dumps(func)
        # safe_func = dill.loads(pickled_func)

        ret = []

        if self.exec_policy == "CLOUD":
            print("CLOUD")
            batch_size = 3
            avg_local_run_time = 0
        else:
            start_timing_loop = time.perf_counter()
            while time.perf_counter() - start_timing_loop < INITIAL_RUN_DUR:
                try:
                    item = iterable.pop(0)
                except IndexError:
                    self.log.info(f"Fastmap finished faster than a loop")
                    return ret, None
                ret.append(func(item))
            avg_local_run_time = (time.perf_counter() - start_timing_loop) / len(ret)

            # print("b", time.perf_counter() - start)

            estimated_run_time = avg_local_run_time * orig_len

            if estimated_run_time < self.num_threads * PROCESS_OVERHEAD + estimated_run_time / self.num_threads:
                self.log.info("Running single threaded")
                return list(map(func, iterable)), avg_local_run_time

            # print("c", time.perf_counter() - start)

            batch_size = int(BATCH_DUR / avg_local_run_time)

            if len(iterable) // self.num_threads < batch_size:
                self.log.info("Running via map because it's not worth uploading")
                with multiprocessing.Pool(self.num_threads) as pool:
                    ret = pool.map(func, iterable)
                    # print("d", time.perf_counter() - start)
                    return ret, avg_local_run_time

        processors, batches, results = self.get_processors(func)
        print("processors", processors)
        # be sure we are at least spreading it out equally amongst all processes
        # TODO less than 5 seconds means run local maybe?
        # batch_size = min((BATCH_DUR // self.avg_local_run_time,
        #                   len(iterable) // self.num_threads))

        in_cnt = 0
        self.log.info("Batch size: %d/%d", batch_size, len(iterable))
        for batch in gen_batches(iterable, batch_size):
            print("batch", batch)
            batches.put(batch)
            in_cnt += 1

        # poison pill
        for _ in range(len(processors)):
            batches.put(None)

        out_cnt = 0
        while out_cnt < in_cnt:
            percent = (out_cnt / in_cnt) * 100
            sys.stdout.write('%s %.1f%%\r' % ('█' * int(percent / 2), percent))
            sys.stdout.flush()
            ret += results.get()
            out_cnt += 1
        sys.stdout.write('%s %.1f%%\n' % ('█' * 50, 100))
        sys.stdout.flush()

        for p in processors:
            p.join()

        return ret, avg_local_run_time

        # f1 = None
        # ret = []
        # for i, batch in enumerate(gen_batches(iterable, self.num_threads*10)):
        #     if not f1:
        #         f1 = self._cloud_run(encoded_function, batch)
        #         print(f1, f1.__dict__)
        #         continue

        #     if f1.exception():
        #         pass

        #     if f1.done():
        #         resp = f1.result()
        #         if resp.status_code != 200:
        #             raise AssertionError("Error: %r" % resp.reason)
        #         # self.log.info("Responded in %.2f", resp.json()['elapsed_time'])
        #         ret += decode(resp.json().get('result'))
        #         f1 = self._cloud_run(encoded_function, batch)
        #         continue
        #     with Pool(self.num_threads) as pool:
        #         ret += pool.map(func, batch)

        # return ret

    # def _cloud_run(self, encoded_function, data):
    #     session = FuturesSession()
    #     encoded_data = encode(data)
    #     # self.log.info(f"Data payload {len(encoded_data):n} bytes")
    #     payload = {'func': encoded_function, 'data': encoded_data}
    #     return session.post(self.cloud_url_base + '/api/v1/execute', json=payload)

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
            self.log.setLevel(logging.WARNING)
        elif verbosity == Verbosity.LOUD:
            self.log.setLevel(logging.INFO)
        else:
            raise AssertionError(f"Unknown value for verbosity '{verbosity}'")
        self.verbosity = verbosity

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s: %(message)s')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)

    # def fastmap(self, func, iterable):
    #     main_timer = timeit.Timer()

    #     iterable = list(iterable)  # TODO
    #     if not iterable:
    #         self.log.warning("Returning empty set due to empty iterable")
    #         return []

    #     self.log.info(f"Mapping {func.__name__} to {len(iterable)} items")

    #     # if isinstance(iterable, types.GeneratorType): # TODO
    #     #     first_batch = list(itertools.islice(iterable, num_threads))
    #     first_batch = iterable[:self.num_threads]
    #     iterable = iterable[self.num_threads:]

    #     mapped_results = []
    #     with Pool(self.num_threads) as local_pool:
    #         timed_func_results = local_pool.map(lambda e: _timed_func(func, e), first_batch)
    #     avg_local_dur = sum(r[0] for r in timed_func_results) / self.num_threads
    #     mapped_results += [r[1] for r in timed_func_results]

    #     serialized_bytes = dill.dumps(func)
    #     func_hash = _gen_hash(serialized_bytes)

    #     if func_hash in _transfer_durs:
    #         avg_transfer_dur = _transfer_durs[func_hash]
    #     else:
    #         start_time = time.time()
    #         post_payload = json.dumps({"func": serialized_bytes, "data": first_batch})
    #         resp = post_json(Defaults.CLOUD_URL + "/api/v1/init_map", post_payload)
    #         elapsed_time = time.time() - start_time
    #         transfer_time = elapsed_time - resp['elapsed_time']

    #     avg_transfer_dur = 0  # TODO
    #     if True:  # avg_local_dur < _config.avg_transfer_dur():
    #         self.log.info(f"Local-execution-time < item-transfer-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing entire map locally"),
    #         with Pool(self.num_threads) as local_pool:
    #             mapped_results += local_pool.map(func, iterable)
    #         self.log.info(f"Processed {len(iterable)} items in {main_timer.timeit()}s")
    #         return mapped_results

    #     self.log.info(f"item-transfer-time < local-execution-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing on cloud and locally"),


# def _timed_func(func, *args):
#     t = timeit.Timer()
#     ret = func(*args)
#     return (t.timeit(), ret)


# def _gen_hash(serialized_bytes):
#     sha = hashlib.sha256()
#     sha.update(serialized_bytes)
#     return sha.hexdigest()
