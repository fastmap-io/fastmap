import base64
import logging
from multiprocessing import Pool

# import capnp
# import fastmap_capnp
import dill
import psutil
from requests_futures.sessions import FuturesSession

_transfer_durs = {}
dill.settings['recurse'] = True


FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the mapped results.

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in almost all circumstances.

    Processing can occur locally via multithreading or on the cloud via the fastmap.io
    service. Often, both the local and cloud environments will be utilized
    simultaneously. If not explicitly specified, the division of work between the
    local environment and the cloud will be determined automatically. If any processing
    is performed on the cloud, charges will apply according to the current pricing.
    For documentation on fastmap's cloud service, visit https://fastmap.io/cloud.

    Configuration is possible by either:
    - initializing configuration globally with fastmap_global_init
    - initializing configuration for one reusable FastmapConfig object with fastmap_init

    For more documentation on configuration, please refer to the docstring for fastmap_init.

    Fastmap assumes that calls to the mapped function will be deterministic for
    a given iterable element. As a result, processing may be done out-of-order.
    """

FASTMAP_INIT_DOCSTRING = """
    Named parameters:
    token: Available to be generated from fastmap.io/generate_token. If not included, fastmap will default to running locally
    verbosity: One of 'QUIET', 'NORMAL', or 'LOUD'. Default is 'normal'.
    exec_policy: Where to process the map. One of 'LOCAL', 'CLOUD', or 'ADAPTIVE' (default).
    all_local_cpus: By default, fastmap will utilize n - 1 avaiable threads. This is to allow your computer to remain performant for other tasks while fastmap is running. Passing 'all_local_cpus = True' will make fastmap run faster at the cost of usability for other tasks.
    confirm_charges: Before processing on the cloud, manually confirm cloud charges.
    """


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


def gen_batches(data, batch_size):
    for i in range(len(data) // batch_size):
        yield data[i*batch_size:(i+1)*batch_size]


class FastmapConfig(object):
    """
    The configuration object. Do not instantiate this directly. Instead, use fastmap_init or
    fastmap_global_init.

    This object exposes one public method: fastmap.
    """

    def __init__(self, token=None, verbosity=Verbosity.NORMAL, exec_policy=ExecPolicy.ADAPTIVE,
                 confirm_charges=False, all_local_cpus=False):
        self.token = token
        self.exec_policy = exec_policy
        self.all_local_cpus = all_local_cpus
        self._init_logging(verbosity)

        self.log.info("Setup fastmap")
        self.log.info(f" verbosity: {verbosity}.")
        self.log.info(f" exec_policy: {exec_policy}")

        self.confirm_charges = confirm_charges
        self.cloud_url_base = "https://fastmap.io"

        # self.client = capnp.TwoPartyClient(Defaults.CLOUD_URL)
        # self.fastmap_wrapper = self.client.bootstrap().cast_as(fastmap_capnp.Fastmap)

        try:
            self.num_threads = len(psutil.Process().cpu_affinity())
        except AttributeError:
            self.num_threads = psutil.cpu_count()
        # self.num_threads = cpu_count()
        if not self.all_local_cpus:
            self.num_threads -= 1

        if not confirm_charges and exec_policy != ExecPolicy.LOCAL:
            self.log.warning("WARNING: 'confirm_charges' is False. Your prepaid fastmap.io balance "
                             "may be charged for usage without confirmation")

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, data):
        data = list(data)
        if not data:
            return []
        self.log.info(f"Running fastmap on {func.__name__} with {len(data)} elements")

        encoded_function = encode(func)
        self.log.info(f"Function payload {len(encoded_function):n} bytes")

        f1 = None
        ret = []
        for batch in gen_batches(data, self.num_threads*10):
            if not f1:
                f1 = self._cloud_run(encoded_function, batch)
                continue

            if f1 and f1.done():
                resp = f1.result()
                if resp.status_code != 200:
                    raise AssertionError("Error: %r" % resp.reason)
                self.log.info("Responded in %.2f", resp.json()['elapsed_time'])
                ret += decode(resp.json().get('result'))
                f1 = self._cloud_run(encoded_function, batch)
                continue
            with Pool(self.num_threads) as pool:
                ret += pool.map(func, batch)

        return ret

    def _cloud_run(self, encoded_function, data):
        session = FuturesSession()
        encoded_data = encode(data)
        self.log.info(f"Data payload {len(encoded_data):n} bytes")
        payload = {'func': encoded_function, 'data': encoded_data}
        return session.post(self.cloud_url_base + '/api/v1/execute', json=payload)

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
