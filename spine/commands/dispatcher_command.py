import logging
import os
import time
from logging.handlers import RotatingFileHandler

import click

import frappe
from spine.spine_adapter.kafka_client.kafka_async_consumer import KafkaAsyncSingletonConsumer

loggers = {}
default_log_level = logging.DEBUG
LOG_FILENAME = '../logs/event_dispatcher.log'

logger = None

MAX_BACKOFF = 300  # 5 minutes
INITIAL_BACKOFF = 5  # 5 seconds


def get_logger(module):
    if module in loggers:
        return loggers[module]

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s | %(pathname)s:%(message)s\n')
    # handler = logging.StreamHandler()

    handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=100000, backupCount=20)
    handler.setFormatter(formatter)

    logger = logging.getLogger(module)
    logger.setLevel(frappe.log_level or default_log_level)
    logger.addHandler(handler)
    logger.propagate = False

    loggers[module] = logger

    return logger


def _validate_site(site):
    """Check if the site directory exists before attempting frappe.init."""
    sites_dir = os.path.abspath(os.path.join(os.getcwd(), 'sites'))
    site_path = os.path.join(sites_dir, site)
    if not os.path.isdir(site_path):
        raise FileNotFoundError("Site {} does not exist at {}".format(site, site_path))


def start_dispatchers(site, queue="kafka_events", type="json", quiet=False, **kargs):
    global logger
    logger = get_logger(__name__)
    logger.debug("Starting worker process.")
    backoff = INITIAL_BACKOFF
    while True:
        try:
            dispatcher(site, queue, type, quiet, logger)
            backoff = INITIAL_BACKOFF  # reset on successful run
        except FileNotFoundError as e:
            logger.error("Site validation failed: {}. Retrying in {} seconds.".format(e, backoff))
            print("Site validation failed: {}. Retrying in {} seconds.".format(e, backoff))
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
        except Exception as e:
            logger.error("Dispatcher error: {}. Retrying in {} seconds.".format(e, backoff))
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


def dispatcher(site, queue, type="json", quiet=False, log=None):
    _validate_site(site)
    frappe.init(site=site)
    frappe_logger = frappe.logger(__name__, with_more_info=False)
    frappe_logger.info(
        "Event Dispatcher starting with pid {}. Process will log to {}".format(os.getpid(), LOG_FILENAME))

    try:
        logger.debug("Starting dispatcher worker.")
        KafkaAsyncSingletonConsumer(log=log).start_dispatcher(queue, type, site, quiet)
    finally:
        frappe.destroy()
