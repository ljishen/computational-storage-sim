#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from collections import namedtuple
from functools import wraps
from os import environ
from random import SystemRandom

import simpy

MICROSECOND = 1


class NetworkFabric:
    _NETWORK_BW_GBPS = 1

    def __init__(self, env):
        self.env = env
        self.cable = simpy.Container(env,
                                     capacity=self._NETWORK_BW_GBPS * 1e9 / 8)

        self.endpoints = {}

    def connect(self, name, endpoint):
        if not issubclass(type(endpoint), Memorable):
            raise RuntimeError(
                "{} is not a memorable endpoint".format(endpoint))

        self.endpoints[name] = endpoint

    def send(self, endpoint_name, data_packet):
        if endpoint_name not in self.endpoints:
            raise RuntimeError(
                "network fabric has no connection to '{}'".format(
                    endpoint_name))

        yield self.cable.put(data_packet.length)

        yield self.env.timeout(data_packet.length / self.cable.capacity * 1e9)
        yield self.cable.get(data_packet.length)

        self.endpoints[endpoint_name].memorize(data_packet.command_id,
                                               data_packet)


class Memorable(ABC):
    def __init__(self):
        self.cache = {}

    def memorize(self, key, data):
        self.cache[key] = data

    def read_cache(self, key):
        return self.cache.pop(key)


# pylint: disable=too-few-public-methods
class EmbeddedPlatform(Memorable):
    ENDPOINT_NAME = "embedded"

    _NUM_PROCESSORS_EMBEDDED = 2
    _DATA_ACCESS_DELAY_PER_BYTE_EMBEDDED = MICROSECOND
    _COMPUTE_DELAY_PER_BYTE_EMBEDDED = MICROSECOND * 2

    def __init__(self, env, network_fabric, submission_queues,
                 completion_queues):
        super().__init__()

        self.logger = LOGGER.bind(component=self.ENDPOINT_NAME)

        self.env = env

        self.network_fabric = network_fabric
        network_fabric.connect(self.ENDPOINT_NAME, self)
        self.logger.debug("connected to network fabric")

        self.submission_queues = submission_queues
        self.completion_queues = completion_queues

        self.processors = simpy.Store(env,
                                      capacity=self._NUM_PROCESSORS_EMBEDDED)

    def __do_process(self, processor_id, comm, completion_queue):
        # emulate the time for data access
        yield self.env.timeout(comm.length *
                               self._DATA_ACCESS_DELAY_PER_BYTE_EMBEDDED)

        ret_isp = False
        ret_length = comm.length
        if comm.req_isp:
            # emulate the time for in-storage processing
            yield self.env.timeout(comm.length *
                                   self._COMPUTE_DELAY_PER_BYTE_EMBEDDED)
            ret_isp = True
            ret_length = comm.length * (1 - comm.selectivity)

        yield self.env.process(
            self.network_fabric.send(HostPlatform.ENDPOINT_NAME,
                                     DataPacket(comm.id, ret_length)))

        # place the CompletionCommand on the completion queue
        # of the same core
        comp_comm = CompletionCommand(id=comm.id,
                                      length=ret_length,
                                      ret_isp=ret_isp,
                                      submit_timestamp=comm.timestamp)
        yield completion_queue.put(comp_comm)

        self.logger.bind(by_processor=processor_id).trace(comp_comm)

    def __process_command(self, submission_queue, completion_queue):
        while True:
            with submission_queue.get() as slot:
                comm = yield slot

                # request an available embedded processor to process command
                processor_id = yield self.processors.get()
                try:
                    yield self.env.process(
                        self.__do_process(processor_id, comm,
                                          completion_queue))
                finally:
                    # release the processor
                    yield self.processors.put(processor_id)

    def start(self):
        for idx in range(self._NUM_PROCESSORS_EMBEDDED):
            self.processors.put(idx)

        for idx in range(len(self.submission_queues)):
            self.env.process(
                self.__process_command(self.submission_queues[idx],
                                       self.completion_queues[idx]))

        self.logger.debug("platform has started")


class HostPlatform(Memorable):
    ENDPOINT_NAME = "host"
    NUM_PROCESSORS_HOST = 24

    _COMPUTE_DELAY_PER_BYTE_HOST = MICROSECOND

    _IO_SUBMISSION_Q_SIZE_PER_CORE = 64 * 1e3  # NVMe specification
    _IO_COMPLETION_Q_SIZE_PER_CORE = 64 * 1e3

    def __init__(self, env, network_fabric):
        super().__init__()

        self.logger = LOGGER.bind(component=self.ENDPOINT_NAME)

        self.env = env

        self.network_fabric = network_fabric
        network_fabric.connect(self.ENDPOINT_NAME, self)
        self.logger.debug("connected to network fabric")

        self.submission_queues = []
        self.completion_queues = []

    def __process_command(self, processor_id):
        while True:
            with self.completion_queues[processor_id].get() as slot:
                comm = yield slot
                if not comm.ret_isp:
                    data_packet = self.read_cache(comm.id)
                    yield self.env.timeout(data_packet.length *
                                           self._COMPUTE_DELAY_PER_BYTE_HOST)
                    self.logger.bind(by_processor=processor_id).trace(comm)

    def start(self):
        for idx in range(self.NUM_PROCESSORS_HOST):
            self.submission_queues.append(
                simpy.Store(self.env,
                            capacity=self._IO_SUBMISSION_Q_SIZE_PER_CORE))
            self.completion_queues.append(
                simpy.Store(self.env,
                            capacity=self._IO_COMPLETION_Q_SIZE_PER_CORE))

            self.env.process(self.__process_command(idx))

        self.logger.debug("platform has started with established " +
                          "submission_queues and completion_queues")

    def get_submission_queues(self):
        return self.submission_queues

    def get_completion_queues(self):
        return self.completion_queues


# pylint: disable=too-few-public-methods
class App:
    _SUBMIT_COMM_DELAY_PER_BYTE = MICROSECOND

    def __init__(self, env, submission_queues):
        self.env = env
        self.submission_queues = submission_queues

        self.cryptogen = SystemRandom()

    def __start(self, app_idx, comms_per_app):
        for comm_idx in range(
                self.cryptogen.randrange(comms_per_app[1] - comms_per_app[0]) +
                comms_per_app[0]):
            data_length = 4 * 1024 * 10  # 4 MiB data size

            # emulate the time taken to generate the data
            yield self.env.timeout(data_length *
                                   self._SUBMIT_COMM_DELAY_PER_BYTE)

            submit_comm = SubmissionCommand(
                "{:d}-{:d}".format(app_idx, comm_idx), data_length, True, 0.7,
                self.env.now)
            submission_queue = self.submission_queues[
                app_idx % HostPlatform.NUM_PROCESSORS_HOST]
            yield submission_queue.put(submit_comm)

    def start_batch(self, num, comms_per_app=(500, 1000)):
        """
        Start a number of apps to generate submission commands.

        Args:
        ----
            num (int): The number of apps to start.
            comms_per_app: (int, int):
                The number of commands in this range an app will generate.
                If (a, b) is the range, the number of commands to generate
                will be a <= x < b.

        """
        for app_idx in range(num):
            self.env.process(self.__start(app_idx, comms_per_app))


# pylint: disable=too-few-public-methods
class EventTracer:
    def __init__(self, env):
        self.env = env
        self.trace_data = []

    def trace(self):  # noqa
        """Replace the ``step()`` method of *env* with a tracing function
        that calls *callbacks* with an events time, priority, ID and its
        instance just before it is processed.

        """

        # pylint: disable=protected-access,invalid-name
        def get_wrapper(env_step):
            """Generate the wrapper for env.step()."""
            @wraps(env_step)
            def tracing_step():  # noqa
                """Call *__monitor* for the next event if one exist before
                calling ``env.step()``."""
                if len(self.env._queue) > 0:
                    t, prio, eid, event = self.env._queue[0]
                    self.__monitor(t, prio, eid, event)
                return env_step()

            return tracing_step

        self.env.step = get_wrapper(self.env.step)

    def __monitor(self, t, prio, eid, event):
        # pylint: disable=invalid-name,unused-argument
        if isinstance(event, simpy.resources.store.StoreGet):
            self.trace_data.append(
                (t, eid, event.proc, type(event), event.resource.items[0]
                 if len(event.resource.items) > 0 else None,
                 event.resource.capacity))
        elif isinstance(event, simpy.resources.store.StorePut):
            self.trace_data.append((t, eid, event.proc, type(event),
                                    event.item, event.resource.capacity))
        else:
            self.trace_data.append((
                t,
                eid,
                event.proc if hasattr(event, 'proc') else None,
                type(event),
                event,
            ))

    def print_trace_data(self, tag="new"):
        print(("==================== "
               "TRACE DATA (BEGIN) (tag: {}) "
               "====================").format(tag))
        for data in self.trace_data:
            print(data)
        print(("==================== "
               "TRACE DATA  (END)  (tag: {}) "
               "====================\n").format(tag))


TRACER = None
LOGGER = None


def main():
    env = simpy.Environment()

    # pylint: disable=global-statement
    global LOGGER
    LOGGER = logger.patch(
        lambda record: record["extra"].update(current=env.now))

    network_fabric = NetworkFabric(env)

    host_platform = HostPlatform(env, network_fabric)
    host_platform.start()

    embedded_platform = EmbeddedPlatform(env, network_fabric,
                                         host_platform.get_submission_queues(),
                                         host_platform.get_completion_queues())
    embedded_platform.start()

    App(env, host_platform.get_submission_queues()).start_batch(5, (300, 500))

    # event tracing
    ###############################

    # pylint: disable=global-statement
    global TRACER
    TRACER = EventTracer(env)

    # Uncomment the following line so that you can print
    # the event history with TRACER.print_trace_data() at
    # wherever you want.

    # TRACER.trace()

    ###############################

    env.run()

    # Or you can print the event history at the end.
    # TRACER.print_trace_data()


if __name__ == "__main__":
    environ['LOGURU_FORMAT'] = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>[{extra[component]: ^8}] {message}</level>")

    from loguru import logger

    logger.add("output/data_{time}.log",
               level='TRACE',
               filter=lambda record: 'by_processor' in record['extra'],
               format=("[{extra[component]: <8} p {extra[by_processor]: <2}]"
                       " <level>{message}</level> @ {extra[current]}"),
               enqueue=True)

    # selectivity: range is [0, 1]
    # length: in the unit of byte
    SubmissionCommand = namedtuple(
        'SubmissionCommand', 'id, length, req_isp, selectivity, timestamp')
    CompletionCommand = namedtuple('CompletionCommand',
                                   'id, length, ret_isp, submit_timestamp')
    DataPacket = namedtuple('DataPacket', 'command_id, length')

    with logger.catch():
        main()
