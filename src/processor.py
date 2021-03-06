#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import simpy  # type: ignore

from event_tracer import EventTracer
from logger import LOGGER

MICROSECONDS = 1e6


class Processor:
    # pylint: disable=too-many-instance-attributes
    """
    Simulating a processing unit that can receive and process tasks.

    Args:
    ----
        env (simpy.Environment): The simpy execution environment.
        host (str): The host's name or tag of the processor.
        num_cores (int): The number of cores that this processor has.
        frequency (float): The number of clock cycles that this processor
            can generate per second.
        step_cycles (int): The number of cycles in a step the processor goes.
            Defaults to 1. The parameter is mainly used to accelerate the
            simulation.

    """

    FREQUENCY_MHZ = 1e6
    FREQUENCY_GHZ = FREQUENCY_MHZ * 1e3

    class Task:
        # pylint: disable=too-few-public-methods,invalid-name
        """
        A Task definition for Processor.

        Args:
        ----
            id_ (str): The identity of the task.
            core_id (Union[int, None]): The core the task will be assigned to
                for processing. Defaults to None, which means it will be
                arbitrarily assigned to an available core at runtime.
            cycles (Union[float, int]): The number of cycles the task requests.
            priority (float): The lower the number, the higher the priority of
                the task. Defaults to 0.

        """

        PRIORITY_HIGH = -1
        PRIORITY_DEFAULT = 0
        PRIORITY_LOW = 19

        __slots__ = ('id', 'core_id', 'cycles', 'priority', 'cb_event')

        def __init__(self, id_, core_id, cycles, priority=PRIORITY_DEFAULT):
            self.id = id_

            self.core_id = self.__get_int_type("core_id",
                                               core_id) if core_id else core_id

            self.cycles = self.__get_int_type("cycles", cycles)  # type: int
            self.priority = float(priority)

            self.cb_event = None

        def __get_int_type(self, name, param):
            if int(param) != param:
                raise RuntimeError(self.__class__.__name__ +
                                   "(id=%r) does not have integer %r (=%r)" %
                                   (self.id, name, param))
            return int(param)

        def __repr__(self):
            """Return a nicely formatted representation string."""
            return self.__class__.__name__ + \
                '(id=%r, core_id=%r, cycles=%r, priority=%r, cb_event=%r)' % \
                (self.id,
                 self.core_id,
                 self.cycles,
                 self.priority,
                 self.cb_event)

    def __init__(self, env, host, num_cores, frequency, step_cycles=1):
        # pylint: disable=too-many-arguments

        self.__logger = LOGGER.bind(component="processor-" + host)

        self.__env = env
        self.__host = host

        self.num_cores = num_cores
        self.frequency = frequency

        self.__step_cycles = int(step_cycles)  # type: int

        if self.__step_cycles != step_cycles:
            raise RuntimeError(
                self.__class__.__name__ +
                "(host=%r) does not support non-integer step_cycles (=%r)" %
                (host, step_cycles))

        self.__waiting_tasks = simpy.PriorityStore(env)
        self.__on_core_tasks = {}

    def __repr__(self):
        return self.__class__.__name__ + \
            '(env=%r, host=%r, num_cores=%r, frequency=%r, step_cycles=%r)' % \
            (self.__env,
             self.__host,
             self.num_cores,
             self.frequency,
             self.__step_cycles)

    def __assign(self, core_id, task):
        priority_item = yield self.__waiting_tasks.get()
        assert priority_item.item.id == task.id  # nosec

        self.__on_core_tasks[core_id] = task
        self.__logger.debug("assigned {} to core {:d}", task, core_id)

    def __advance(self):
        cycle_increments = None
        if len(self.__waiting_tasks.items) > 0 or len(
                self.__on_core_tasks) == self.num_cores:
            for task in self.__on_core_tasks.values():
                if cycle_increments:
                    cycle_increments = (cycle_increments
                                        if cycle_increments <= task.cycles else
                                        task.cycles)
                else:
                    cycle_increments = task.cycles
        else:
            cycle_increments = self.__step_cycles

        if cycle_increments > self.__step_cycles:
            self.__logger.warning("fast advance {:d} cycles", cycle_increments)

        yield self.__env.timeout(MICROSECONDS / self.frequency *
                                 cycle_increments)

        completed_core_ids = []
        for core_id, task in self.__on_core_tasks.items():
            task.cycles -= cycle_increments

            if task.cycles == 0:
                completed_core_ids.append(core_id)

        for core_id in completed_core_ids:
            task = self.__on_core_tasks.pop(core_id)
            task.cb_event.succeed(core_id)

    def __loop(self):
        while True:
            remaining_waiting_tasks = simpy.PriorityStore(self.__env)

            while len(self.__waiting_tasks.items) > 0:
                task = self.__waiting_tasks.items[0].item

                assigned = False
                if isinstance(task.core_id, int):
                    if task.core_id not in self.__on_core_tasks:
                        yield self.__env.process(
                            self.__assign(task.core_id, task))
                        assigned = True
                else:
                    for core_id in range(self.num_cores):
                        if core_id not in self.__on_core_tasks:
                            yield self.__env.process(
                                self.__assign(core_id, task))
                            assigned = True
                            break

                if len(self.__on_core_tasks) == self.num_cores:
                    break

                if not assigned:
                    priority_item = yield self.__waiting_tasks.get()
                    yield remaining_waiting_tasks.put(priority_item)

            while len(self.__waiting_tasks.items) > 0:
                priority_item = yield self.__waiting_tasks.get()
                yield remaining_waiting_tasks.put(priority_item)
            self.__waiting_tasks = remaining_waiting_tasks

            yield self.__env.process(self.__advance())

    def run(self):
        self.__env.process(self.__loop())
        self.__logger.debug("{} is running.", str(self))

    def submit(self, task):
        if task.cycles % self.__step_cycles != 0:
            raise RuntimeError(
                ("Cycles of {} is not divisible by the step_cycles (={:d}) "
                 "of the processor. Simulation will be incorrect.").format(
                     str(task), self.__step_cycles))

        task.cb_event = self.__env.event()

        if task.priority == self.Task.PRIORITY_DEFAULT:
            task.priority = self.__env.now

        yield self.__waiting_tasks.put(simpy.PriorityItem(task.priority, task))
        return task.cb_event


TRACER = None


def run_workload(env, processor, shutdown_hook):
    num_tasks = 11
    events = []
    for idx in range(num_tasks):
        cb_event = yield env.process(
            processor.submit(
                Processor.Task(idx,
                               idx % processor.num_cores,
                               1e3,
                               priority=num_tasks - idx)))
        events.append(cb_event)

    for cb_event in events:
        yield cb_event

    shutdown_hook.succeed()


@LOGGER.catch
def main():
    env = simpy.Environment()

    num_cores = 2
    processor = Processor(env,
                          "test",
                          num_cores,
                          2 * Processor.FREQUENCY_GHZ,
                          step_cycles=1e2)
    processor.run()

    shutdown_hook = env.event()
    env.process(run_workload(env, processor, shutdown_hook))

    # event tracing
    ###############################

    # pylint: disable=global-statement
    global TRACER
    TRACER = EventTracer(env)

    # Uncomment the following line so that you can print
    # the event history with TRACER.print_trace_data() at
    # wherever you want.

    TRACER.trace()

    ###############################

    env.run(until=shutdown_hook)

    # Or you can print the event history at the end.
    TRACER.print_trace_data()


if __name__ == "__main__":
    main()
