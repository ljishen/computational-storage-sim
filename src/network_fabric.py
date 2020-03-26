#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import simpy  # type: ignore

from logger import LOGGER
from memorable import Memorable
from processor import MICROSECONDS


class NetworkFabric:
    ONE_GBPS = 1

    def __init__(self, env, gbps):
        self.__logger = LOGGER.bind(component="network-fabric")

        self.__env = env

        # capacity is in the unit of kilobyte
        self.__cable = simpy.Container(env, capacity=gbps * 1e9 / 8 / 1e3)

        self.__endpoints = {}

    def connect(self, name, endpoint):
        if len(self.__endpoints) == 2:
            raise RuntimeError(
                "this network fabric is already used by {}".format(
                    list(self.__endpoints.keys())))

        if not issubclass(type(endpoint), Memorable):
            raise RuntimeError("{} is not a memorable endpoint".format(
                str(endpoint)))

        self.__endpoints[name] = endpoint

        if len(self.__endpoints) == 2:
            self.__logger.debug(
                "network connection has established between {}",
                list(self.__endpoints.keys()))

    def get_endpoint(self, endpoint_name):
        if endpoint_name not in self.__endpoints:
            raise RuntimeError(
                "this network fabric is not connected to '{}'".format(
                    endpoint_name))
        return self.__endpoints[endpoint_name]

    def request(self, length):
        yield self.__cable.put(length)

    def release(self, length):
        yield self.__cable.get(length)

    def transmit(self, length):
        yield self.__env.timeout(length / self.__cable.capacity * MICROSECONDS)
