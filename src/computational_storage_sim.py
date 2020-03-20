#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple
from random import SystemRandom

import simpy

from loguru import logger

MICROSECOND = 1


class NetworkFabric:
    _NETWORK_BW_GBPS = 1

    def __init__(self, env):
        self.env = env
        self.cable = simpy.Container(env,
                                     capacity=self._NETWORK_BW_GBPS * 1e9 / 8)

        # host memory, assuming it is big enough to host any data
        self.memory = simpy.Store(env)

    def send(self, data_packet):
        # yield self.cable.put(data_packet.length)
        yield self.env.timeout(data_packet.length / self.cable.capacity * 1e6)

        # write data to memory
        # self.memory.put(data_packet)

    def receive(self):
        # read from local memory
        data_packet = yield self.memory.get()
        yield self.cable.get(data_packet.length)
        return data_packet


class EmbeddedPlatform:
    _NUM_PROCESSORS_EMBEDDED = 2
    _DATA_ACCESS_DELAY_PER_BYTE_EMBEDDED = MICROSECOND
    _COMPUTE_DELAY_PER_BYTE_EMBEDDED = MICROSECOND * 2

    def __init__(self, env, network_fabric, submission_queues,
                 completion_queues):
        self.env = env
        self.network_fabric = network_fabric
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
            self.network_fabric.send(DataPacket(comm.id, ret_length)))

        # place the CompletionCommand on the completion queue
        # of the same core
        comp_comm = CompletionCommand(id=comm.id,
                                      length=ret_length,
                                      ret_isp=ret_isp,
                                      submit_timestamp=comm.timestamp)
        yield completion_queue.put(comp_comm)

        logger.bind(platform="embedded",
                    by_processor=processor_id).success(comp_comm)

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

        logger.debug("embedded platform has started")


class HostPlatform:
    NUM_PROCESSORS_HOST = 24

    _COMPUTE_DELAY_PER_BYTE_HOST = MICROSECOND

    _IO_SUBMISSION_Q_SIZE_PER_CORE = 64 * 1e3  # NVMe specification
    _IO_COMPLETION_Q_SIZE_PER_CORE = 64 * 1e3

    def __init__(self, env, network_fabric):
        self.env = env
        self.network_fabric = network_fabric

        self.submission_queues = []
        self.completion_queues = []

    def __process_command(self, processor_id):
        while True:
            with self.completion_queues[processor_id].get() as slot:
                comm = yield slot
                if not comm.ret_isp:
                    # need to read data from NetworkFabric
                    yield self.env.timeout(comm.length *
                                           self._COMPUTE_DELAY_PER_BYTE_HOST)
                    logger.bind(platform="host",
                                by_processor=processor_id).success(comm)

    # def __poll_data(self, network_fabric):
    #     logger.debug('waiting to receive network data...')
    #     while True:
    #         network_fabric.receive()

    def start(self):
        for idx in range(self.NUM_PROCESSORS_HOST):
            self.submission_queues.append(
                simpy.Store(self.env,
                            capacity=self._IO_SUBMISSION_Q_SIZE_PER_CORE))
            self.completion_queues.append(
                simpy.Store(self.env,
                            capacity=self._IO_COMPLETION_Q_SIZE_PER_CORE))

            self.env.process(self.__process_command(idx))

        logger.debug("host platform has started with established " +
                     "submission_queues and completion_queues")

    def get_submission_queues(self):
        return self.submission_queues

    def get_completion_queues(self):
        return self.completion_queues


class App:
    def __init__(self, env, submission_queues):
        self.env = env
        self.submission_queues = submission_queues

        self.cryptogen = SystemRandom()
        self.counter = 0

    def __start(self, app_idx):
        for comm_idx in range(self.cryptogen.randrange(900) + 100):
            submit_comm = SubmissionCommand(
                "{:d}-{:d}".format(app_idx, comm_idx), 4 * 1024, False, 0,
                self.env.now)
            submission_queue = self.submission_queues[
                app_idx % HostPlatform.NUM_PROCESSORS_HOST]
            yield submission_queue.put(submit_comm)

    def start_batch(self, num):
        for app_idx in range(num):
            self.env.process(self.__start(app_idx))


@logger.catch
def main():
    env = simpy.Environment()

    network_fabric = NetworkFabric(env)

    host_platform = HostPlatform(env, network_fabric)
    host_platform.start()

    embedded_platform = EmbeddedPlatform(env, network_fabric,
                                         host_platform.get_submission_queues(),
                                         host_platform.get_completion_queues())
    embedded_platform.start()

    App(env, host_platform.get_submission_queues()).start_batch(1)

    env.run()


if __name__ == "__main__":
    logger.add("output/data.log",
               level='SUCCESS',
               format="[{extra[platform]: <8} @ {extra[by_processor]: <2}]" +
               " <level>{message}</level>",
               enqueue=True)

    # selectivity: range is [0, 1]
    # length: in the unit of byte
    SubmissionCommand = namedtuple(
        'SubmissionCommand', 'id, length, req_isp, selectivity, timestamp')
    CompletionCommand = namedtuple('CompletionCommand',
                                   'id, length, ret_isp, submit_timestamp')
    DataPacket = namedtuple('DataPacket', 'command_id, length')

    main()
