#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC


class Memorable(ABC):
    def __init__(self):
        self.__cache = {}

    def memorize(self, key, data):
        self.__cache[key] = data

    def read_cache(self, key):
        return self.__cache.pop(key)
