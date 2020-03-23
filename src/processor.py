#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple

import simpy  # type: ignore

from event_tracer import EventTracer  # type: ignore
from logger import LOGGER

MICROSECOND = 1


class Processor:
    FREQUENCY_MHZ = 1e6
    FREQUENCY_GHZ = FREQUENCY_MHZ * 1e3

    Task = namedtuple('Task', 'id, core_id, cycles, cb_event')
    _Core = namedtuple('Core', 'id, availability')

    def __init__(self, env, num_cores, frequency):
        self.__env = env

        self.__cores = []
        for idx in range(num_cores):
            core = self._Core(
                idx, simpy.Container(env, init=frequency, capacity=frequency))
            self.__cores.append(core)

        self.__tasks = simpy.Store(env)

    def __exec(self, _core, task):
        executed = False
        if _core.availability.level >= task.cycles:
            _task = yield self.__tasks.get()
            assert _task.id == task.id  # nosec
            yield _core.availability.get(task.cycles)
            task.cb_event.succeed(_core.id)
            executed = True

        return executed

    def __do_run(self):
        while True:
            while True:
                if len(self.__tasks.items) == 0:
                    break

                task = self.__tasks.items[0]
                executed = False

                if isinstance(task.core_id, int):
                    _core = self.__cores[task.core_id]
                    executed = yield self.__env.process(
                        self.__exec(_core, task))
                else:
                    for _core in self.__cores:
                        executed = yield self.__env.process(
                            self.__exec(_core, task))
                        if executed:
                            break

                if not executed:
                    break

            yield self.__env.timeout(MICROSECOND * 1e9)

            for _core in self.__cores:
                if _core.availability.level < _core.availability.capacity:
                    yield _core.availability.put(_core.availability.capacity -
                                                 _core.availability.level)

    def run(self):
        self.__env.process(self.__do_run())

    def submit(self, task):
        if not isinstance(task.cb_event, simpy.Event):
            raise RuntimeError(
                "cb_event of task {} is not an instance of simpy.Event".format(
                    task))

        if task.cycles > self.__cores[0].availability.capacity:
            raise RuntimeError(
                "This processor cannot afford task {}".format(task))

        yield self.__tasks.put(task)


TRACER = None


def test(env):
    num_cores = 2
    processor = Processor(env, num_cores, 2 * Processor.FREQUENCY_GHZ)
    processor.run()

    num_tasks = 10
    events = []
    for idx in range(num_tasks):
        event = env.event()
        events.append(event)
        env.process(
            processor.submit(
                Processor.Task(idx, idx % num_cores, Processor.FREQUENCY_GHZ,
                               event)))

    return events


@LOGGER.catch
def main():
    env = simpy.Environment()
    events = test(env)

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

    env.run(simpy.AllOf(env, events))

    # Or you can print the event history at the end.
    TRACER.print_trace_data()


if __name__ == "__main__":
    main()
