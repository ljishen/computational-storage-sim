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
    _CPU_CYCLES_PER_DATA_ACCESS = _CORE_FREQUENCY * 0.004

    _CPU_CYCLES_PER_KILOBYTE_COMPUTE = 1e1 * 1e3
    _CPU_CYCLES_PER_KILOBYTE_SEND = 1e1 * 1e2
    _CPU_CYCLES_PER_KILOBYTE_RECEIVE = 1e1 * 1e3

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
            step_cycles=min(self._CPU_CYCLES_PER_KILOBYTE_COMPUTE,
                            self._CPU_CYCLES_PER_KILOBYTE_SEND,
                            self._CPU_CYCLES_PER_KILOBYTE_RECEIVE))

        self.__submission_queues = submission_queues
        self.__completion_queues = completion_queues

    def __network_send(self, endpoint_name, data_packet):
        endpoint = self.__network_fabric.get_endpoint(endpoint_name)

        yield self.__env.process(
            self.__network_fabric.request(data_packet.length))
        try:
            send_event = yield self.__env.process(
                self.__processor.submit(
                    Processor.Task(
                        data_packet.id + "-SEND", None, data_packet.length *
                        self._CPU_CYCLES_PER_KILOBYTE_SEND,
                        Processor.Task.PRIORITY_HIGH)))

            receive_event = yield self.__env.process(
                endpoint.network_receive(data_packet))

            transmit_event = self.__env.process(
                self.__network_fabric.transmit(data_packet.length))

            yield send_event & receive_event & transmit_event
        finally:
            endpoint.memorize(data_packet.id, data_packet)
            yield self.__env.process(
                self.__network_fabric.release(data_packet.length))

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
                        comm.length * self._CPU_CYCLES_PER_KILOBYTE_COMPUTE,
                        Processor.Task.PRIORITY_HIGH)))
            core_id = yield event
            ret_isp = True
            ret_length = comm.length * (1 - comm.selectivity)

        yield self.__env.process(
            self.__network_send(HostPlatform.ENDPOINT_NAME,
                                DataPacket(comm.id, queue_idx, ret_length)))

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


class HostPlatform(Memorable):
    # pylint: disable=too-many-instance-attributes

    ENDPOINT_NAME = "host"

    _CPU_CYCLES_PER_KILOBYTE_GENERATE = 1e1 * 1e3
    _CPU_CYCLES_PER_KILOBYTE_COMPUTE = 1e1 * 1e3

    _CPU_CYCLES_PER_KILOBYTE_SEND = 1e1 * 1e2
    _CPU_CYCLES_PER_KILOBYTE_RECEIVE = 1e1 * 1e3

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
                            self._CPU_CYCLES_PER_KILOBYTE_COMPUTE,
                            self._CPU_CYCLES_PER_KILOBYTE_SEND,
                            self._CPU_CYCLES_PER_KILOBYTE_RECEIVE))

        self.submission_queues = []
        self.completion_queues = []

        self.__all_command_submitted = None
        self.__shutdown_hook = None
        self.summary = {
            "num_commands_submitted": 0,
            "num_commands_completed": 0,
            "total_data_bytes": 0
        }

    def network_receive(self, data_packet):
        event = yield self.__env.process(
            self.__processor.submit(
                Processor.Task(
                    data_packet.id + "-RECEIVE", data_packet.queue_idx,
                    data_packet.length * self._CPU_CYCLES_PER_KILOBYTE_RECEIVE,
                    Processor.Task.PRIORITY_HIGH)))
        return event

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
                                self._CPU_CYCLES_PER_KILOBYTE_COMPUTE,
                                Processor.Task.PRIORITY_HIGH)))
                    core_id = yield event
                    assert core_id == queue_idx  # nosec

                    self.__logger.bind(by_core=core_id).trace(comm)

                self.summary["num_commands_completed"] += 1

                if (self.summary["num_commands_submitted"] ==
                        self.summary["num_commands_completed"]
                        and self.__all_command_submitted.processed):
                    self.__shutdown_hook.succeed()

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
        self.__shutdown_hook = shutdown_hook

    def __app(self, app_idx, comms_per_app, event_submitted):
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

        event_submitted.succeed()

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
        events = []
        for app_idx in range(num):
            event_submitted = self.__env.event()
            self.__env.process(
                self.__app(app_idx, comms_per_app, event_submitted))
            events.append(event_submitted)

        self.__all_command_submitted = simpy.AllOf(self.__env, events)


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

    host_platform.run_apps(24, (20, 30))

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

    DataPacket = namedtuple('DataPacket', 'id, queue_idx, length')

    main()
