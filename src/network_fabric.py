#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple

import simpy  # type: ignore

from logger import LOGGER
from memorable import Memorable
from processor import MICROSECONDS


class NetworkFabric:
    ONE_GBPS = 1

    DataPacket = namedtuple('DataPacket', 'id, length')

    def __init__(self, env, gbps):
        self.__logger = LOGGER.bind(component="network-fabric")

        self.__env = env
        self.__cable = simpy.Container(env, capacity=gbps * 1e9 / 8 / 1e3)

        self.__endpoints = {}

    def connect(self, name, endpoint):
        if not issubclass(type(endpoint), Memorable):
            raise RuntimeError(
                "{} is not a memorable endpoint".format(endpoint))

        self.__endpoints[name] = endpoint
        self.__logger.debug("connection is established for {}", name)

    def send(self, endpoint_name, data_packet):
        if endpoint_name not in self.__endpoints:
            raise RuntimeError(
                "network fabric has no connection to '{}'".format(
                    endpoint_name))

        yield self.__cable.put(data_packet.length)
        try:
            yield self.__env.timeout(data_packet.length /
                                     self.__cable.capacity * MICROSECONDS)
        finally:
            yield self.__cable.get(data_packet.length)

        self.__endpoints[endpoint_name].memorize(data_packet.id, data_packet)