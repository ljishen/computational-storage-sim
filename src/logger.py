#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import environ

environ['LOGURU_FORMAT'] = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>[{extra[component]}] {message}</level>")

# pylint: disable=wrong-import-position
from loguru import logger  # noqa: E402

LOGGER = logger.bind(component="simulator")
