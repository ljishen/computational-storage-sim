#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple
from random import SystemRandom

import simpy  # type: ignore

from event_tracer import EventTracer
from logger import LOGGER
from memorable import Memorable
from network_fabric import NetworkFabric
from processor import Processor


class EmbeddedPlatform(Memorable):
    ENDPOINT_NAME = "embedded"

    _CORE_FREQUENCY = 1.5 * Processor.FREQUENCY_GHZ

    # 1. Because data access is mainly a DMA process, the number of CPU cycles
    #    that are required is not relavent to the size of a data access.
    # 2. CPU utilization for data access is 30% in the worst case. (Source:
    #       Koo, Gunjae, et al. "Summarizer: trading communication with
    #       computing near storage." 2017 50th Annual IEEE/ACM International
    #       Symposium on Microarchitecture (MICRO). IEEE, 2017.)
    _CPU_CYCLES_PER_DATA_ACCESS = _CORE_FREQUENCY * 0.3

    _CPU_CYCLES_PER_KILOBYTE_COMPUTE = 1e1 * 1e3

    def __init__(self, env, network_fabric, submission_queues,
                 completion_queues):
        super().__init__()

        self.__logger = LOGGER.bind(component=self.ENDPOINT_NAME)

        self.__env = env

        self.__network_fabric = network_fabric
        self.__processor = Processor(
            env,
            self.ENDPOINT_NAME,
            2,
            self._CORE_FREQUENCY,
            step_cycles=self._CPU_CYCLES_PER_KILOBYTE_COMPUTE)

        self.__submission_queues = submission_queues
        self.__completion_queues = completion_queues

    def __do_process(self, comm, queue_idx):
        event = yield self.__env.process(
            self.__processor.submit(
                Processor.Task(comm.id + "-ACCESS", None,
                               self._CPU_CYCLES_PER_DATA_ACCESS)))
        yield event

        ret_isp = False
        ret_length = comm.length
        if comm.req_isp:
            event = yield self.__env.process(
                self.__processor.submit(
                    Processor.Task(
                        comm.id + "-COMPUTE", None,
                        comm.length * self._CPU_CYCLES_PER_KILOBYTE_COMPUTE)))
            core_id = yield event
            ret_isp = True
            ret_length = comm.length * (1 - comm.selectivity)

        yield self.__env.process(
            self.__network_fabric.send(
                HostPlatform.ENDPOINT_NAME,
                NetworkFabric.DataPacket(comm.id, ret_length)))

        # place the CompletionCommand on the completion queue
        # of the same core
        comp_comm = CompletionCommand(id=comm.id,
                                      length=ret_length,
                                      req_isp=comm.req_isp,
                                      ret_isp=ret_isp,
                                      submit_timestamp=comm.timestamp)
        yield self.__completion_queues[queue_idx].put(comp_comm)

        if ret_isp:
            self.__logger.bind(by_core=core_id).trace(comp_comm)

    def __process_command(self, queue_idx):
        while True:
            with self.__submission_queues[queue_idx].get() as slot:
                comm = yield slot
                self.__env.process(self.__do_process(comm, queue_idx))

    def start(self):
        self.__processor.run()

        self.__network_fabric.connect(self.ENDPOINT_NAME, self)

        for idx in range(len(self.__submission_queues)):
            self.__env.process(self.__process_command(idx))

        self.__logger.debug("platform has started")


# pylint: disable=too-many-instance-attributes
class HostPlatform(Memorable):
    ENDPOINT_NAME = "host"

    _CPU_CYCLES_PER_KILOBYTE_COMPUTE = 1e1 * 1e3
    _CPU_CYCLES_PER_KILOBYTE_GENERATE = 1e2 * 1e3

    _RECEIVE_SUBMISSION_COMM_DELAY_PER_KILOBYTE = 1e3

    _IO_SUBMISSION_Q_SIZE_PER_CORE = 64 * 1e3  # NVMe specification
    _IO_COMPLETION_Q_SIZE_PER_CORE = 64 * 1e3

    def __init__(self, env, network_fabric):
        super().__init__()

        self.__logger = LOGGER.bind(component=self.ENDPOINT_NAME)

        self.__env = env
        self.__cryptogen = SystemRandom()

        self.__network_fabric = network_fabric
        self.__processor = Processor(
            env,
            self.ENDPOINT_NAME,
            24,
            2.6 * Processor.FREQUENCY_GHZ,
            step_cycles=min(self._CPU_CYCLES_PER_KILOBYTE_GENERATE,
                            self._CPU_CYCLES_PER_KILOBYTE_COMPUTE))

        self.submission_queues = []
        self.completion_queues = []

        self.shutdown_hook = None
        self.summary = {
            "num_commands_submitted": 0,
            "num_commands_completed": 0,
            "total_data_bytes": 0
        }

    def __process_command(self, queue_idx):
        while True:
            with self.completion_queues[queue_idx].get() as slot:
                comm = yield slot
                data_packet = self.read_cache(comm.id)

                if comm.req_isp and not comm.ret_isp:
                    event = yield self.__env.process(
                        self.__processor.submit(
                            Processor.Task(
                                comm.id + "-COMPUTE", queue_idx,
                                data_packet.length *
                                self._CPU_CYCLES_PER_KILOBYTE_COMPUTE)))
                    core_id = yield event
                    assert core_id == queue_idx  # nosec

                    self.__logger.bind(by_core=core_id).trace(comm)

                self.summary["num_commands_completed"] += 1
                if self.summary["num_commands_submitted"] == self.summary[
                        "num_commands_completed"]:
                    self.shutdown_hook.succeed()

    def start(self):
        self.__processor.run()

        self.__network_fabric.connect(self.ENDPOINT_NAME, self)

        for idx in range(self.__processor.num_cores):
            self.submission_queues.append(
                simpy.Store(self.__env,
                            capacity=self._IO_SUBMISSION_Q_SIZE_PER_CORE))
            self.completion_queues.append(
                simpy.Store(self.__env,
                            capacity=self._IO_COMPLETION_Q_SIZE_PER_CORE))

            self.__env.process(self.__process_command(idx))

        self.__logger.debug("platform has started with established " +
                            "submission_queues and completion_queues")

    def set_shutdown_hook(self, shutdown_hook):
        self.shutdown_hook = shutdown_hook

    def __app(self, app_idx, comms_per_app):
        data_length = 4 * 1e3  # 4 MB data size

        for idx in range(
                self.__cryptogen.randrange(comms_per_app[1] -
                                           comms_per_app[0]) +
                comms_per_app[0]):
            queue_idx = self.__cryptogen.randrange(self.__processor.num_cores)

            comm_idx = "{:d}-{:d}-{:d}".format(app_idx, idx, queue_idx)

            # generate the workload at localhost
            event = yield self.__env.process(
                self.__processor.submit(
                    Processor.Task(
                        comm_idx + "-GENERATE", queue_idx,
                        data_length * self._CPU_CYCLES_PER_KILOBYTE_GENERATE)))
            yield event

            # receive the workload by remote hosts
            # yield self.__env.timeout(data_length *
            #                          self._RECEIVE_SUBMISSION_COMM_DELAY_PER_KILOBYTE)

            submit_comm = SubmissionCommand(comm_idx, data_length, True, 0.5,
                                            self.__env.now)
            yield self.submission_queues[queue_idx].put(submit_comm)

            self.summary["num_commands_submitted"] += 1
            self.summary["total_data_bytes"] += data_length

    def run_apps(self, num, comms_per_app=(500, 1000)):
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
            self.__env.process(self.__app(app_idx, comms_per_app))


TRACER = None


@LOGGER.catch
def main():
    env = simpy.Environment()

    # pylint: disable=global-statement
    global LOGGER
    LOGGER.add("output/data_{time}.log",
               level='TRACE',
               filter=lambda record: 'by_core' in record['extra'],
               format=("[{extra[component]: <8} c {extra[by_core]: <2}]"
                       " <level>{message}</level> @ {extra[current]}"),
               enqueue=True)
    LOGGER = LOGGER.patch(
        lambda record: record["extra"].update(current=env.now))

    network_fabric = NetworkFabric(env, NetworkFabric.ONE_GBPS)

    host_platform = HostPlatform(env, network_fabric)
    host_platform.start()

    embedded_platform = EmbeddedPlatform(env, network_fabric,
                                         host_platform.submission_queues,
                                         host_platform.completion_queues)
    embedded_platform.start()

    host_platform.run_apps(24, (10, 20))

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

    shutdown_hook = env.event()
    host_platform.set_shutdown_hook(shutdown_hook)
    env.run(until=shutdown_hook)

    # Or you can print the event history at the end.
    # TRACER.print_trace_data()

    LOGGER.debug("summary: {}".format(host_platform.summary))
    LOGGER.debug("simulation has successfully finished.")


if __name__ == "__main__":
    # selectivity: range is [0, 1]
    # length: in the unit of kilobyte
    SubmissionCommand = namedtuple(
        'SubmissionCommand', 'id, length, req_isp, selectivity, timestamp')
    CompletionCommand = namedtuple(
        'CompletionCommand', 'id, length, req_isp, ret_isp, submit_timestamp')

    main()
