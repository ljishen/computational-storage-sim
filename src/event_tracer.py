#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functools import wraps

import simpy  # type: ignore


class EventTracer:
    def __init__(self, env):
        self.__env = env
        self.__trace_data = []

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
                if len(self.__env._queue) > 0:
                    t, prio, eid, event = self.__env._queue[0]
                    self.__monitor(t, prio, eid, event)
                return env_step()

            return tracing_step

        self.__env.step = get_wrapper(self.__env.step)

    def __monitor(self, t, prio, eid, event):
        # pylint: disable=invalid-name,unused-argument
        if isinstance(event, simpy.resources.store.StoreGet):
            self.__trace_data.append(
                (t, eid, event.proc, type(event), event.resource.items[0]
                 if len(event.resource.items) > 0 else None,
                 event.resource.capacity))
        elif isinstance(event, simpy.resources.store.StorePut):
            self.__trace_data.append((t, eid, event.proc, type(event),
                                      event.item, event.resource.capacity))
        else:
            self.__trace_data.append(
                (t, eid, event.proc if hasattr(event, 'proc') else None,
                 type(event), event, event.value))

    def print_trace_data(self, tag="new"):
        print(("==================== "
               "TRACE DATA (BEGIN) (tag: {}) "
               "====================").format(tag))
        for data in self.__trace_data:
            print(data)
        print(("==================== "
               "TRACE DATA  (END)  (tag: {}) "
               "====================\n").format(tag))
