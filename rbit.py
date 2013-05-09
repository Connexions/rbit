# -*- coding: utf-8 -*-
"""\
A consumer/client implemenation that works with the PyBit system to track
the status of running jobs.

Author: Michael Mulich
Copyright (c) 2012-2013 Rice University

Parts of the client code are derived from the PyBit client implementation at
https://github.com/nicholasdavidson/pybit licensed under GPL2.1.

This software is subject to the provisions of the GNU AFFERO GENERAL PUBLIC LICENSE Version 3.0 (AGPL).  
See LICENSE.txt for details.
"""
import logging
import argparse
import traceback
from importlib import import_module
from logging.config import fileConfig as load_logging_configuration
from ConfigParser import RawConfigParser

import jsonpickle
import pika
import requests
import pybit


CONFIG__HANDLER_PREFIX = 'runner:'
logger = logging.getLogger('rbit')
# Global configuration object
config = None


# ##################### #
#   Public Exceptions   #
# ##################### #

class Blocked(Exception):
    """Occures when a job is blocked from by something
    (e.g. missing data, authentication needed, user input needed)

    """


class Failed(Exception):
    """Occures when a job fails in a known way. This is used by task runners
    to catch and report failure in a way that makes it easier to debug the
    failure.

    """


def parse_runner_line(line):
    """Find suitable importers/handlers for the runner line.
    Return a callables for the given line.

    The line is a single string that has been formatted to display
    its processor type and other information that will be passed to the
    processor of said type. For example::

        python!rbitext.decode_build_request:main

    In this line, we are using the python processor type. In this situation,
    the python processor parses the information about the ``!`` exclamation
    to determine where the logic is located.

    The processor type defines what it will do with the contents after the
    exclamation.

    """
    processor_type, info = line.split('!')
    # FIXME Ideally the processors will be defined in some kind of
    #       registry/configuration.
    #       For now the only supported processor is `python`.
    if processor_type != 'python':
        raise NotImplementedError
    module_path, function = info.split(':')
    package_path = module_path.split('.')
    package = '.'.join(package_path[:-1])
    if not package:
        module = package_path[0]
    else:
        # Relative path to module
        module = '.' + package_path[-1]
    m = import_module(module, package)
    return getattr(m, function)


class Config(object):
    """\
    A configuration agend that holds and acts on information about the
    consumer connection, message queue(s) to follow, and what code &
    additional information should be run & handed to the code when
    a message is recieved (runner settings).

    """

    def __init__(self, queue_mappings, amqp_settings, runners={}):
        self.queue_mappings = queue_mappings
        self.amqp = amqp_settings
        self.runners = runners

    @classmethod
    def from_file(cls, ini_file):
        """Used to initialize the configuration object from an INI file."""
        config = RawConfigParser()
        if hasattr(ini_file, 'read'):
            config.readfp(ini_file)
        else:
            with open(ini_file, 'r') as f:
                config.readfp(f)

        def config_to_dict(c):
            result = {}
            for section in c.sections():
                result[section] = dict(c.items(section))
            return result

        settings = config_to_dict(config)
        rbit = settings['rbit']
        amqp = settings['amqp']

        runners = {}
        for name, value in settings.items():
            if name.startswith(CONFIG__HANDLER_PREFIX):
                name = name[len(CONFIG__HANDLER_PREFIX):]
                runners[name] = value

        # Parse the <queue_name>:<runner_name> lines.
        queue_mappings = [v.strip()
                          for v in rbit.get('queue-mappings', '').split('\n')
                          if v.strip() and v.find(':') >= 1]
        queue_mappings = dict([v.split(':') for v in queue_mappings])
        return cls(queue_mappings, amqp, runners)

def set_status(status, build_request):
    """Update the job's status using the build_request object and global configuration."""
    global config
    payload = {'status': status}
    job_status_url = "http://{0}/job/{1}".format(build_request.web_host,
                                                 build_request.get_job_id())
    # FIXME We need a better way to authenticate... api keys maybe?
    #       We don't really want to be sending status updates via
    #       http anyhow, so this may not be important in the future.
    username = 'admin'
    password = 'pass'
    auth = requests.auth.HTTPBasicAuth(username, password)
    requests.put(job_status_url, payload, auth=auth)

def republish(build_request, queue, channel):
    """Republish or push the build request back to the originating queue."""
    body = jsonpickle.encode(build_request)
    channel.basic_publish(exchange='', routing_key=queue, body=body)

def get_message_handler(settings, queue):
    """This looks up the runner from the settings and wraps it to make it
    a pika compatible message handler. We pass in the queue, because it is
    required for republication of build requests and it cannot be reliably
    derived due to PyBit translation magic (e.g. any becomes any architecture
    distributes to multiple queues).

    """
    runner = parse_runner_line(settings['runner'])

    def message_handler(channel, method, headers, body):
        # Parse the message into a Python object. (Assuming this is a
        #   PyBit BuildRequest.)
        build_request = jsonpickle.decode(body)

        # Update the state of the job in PyBit.
        set_status('Building', build_request)

        # Start the building sequence by updating the build's status.
        job_id = build_request.get_job_id()
        build_request.stamp_request()
        timestamp = build_request.get_buildstamp()
        status_message = "Job '{0}', timestamp: {1}".format(job_id, timestamp)
        logger.info(status_message)

        # Call the runner with the parsed message and settings.
        acknowledge = lambda: channel.basic_ack(method.delivery_tag)
        artifacts = []
        try:
            artifacts = runner(build_request, settings)
            # Acknowledge the message was recieved and processed.
            set_status('Done', build_request)
            acknowledge()
        except (Failed, Blocked) as exc:
            state = exc.__class__.__name__
            logger.info("Job {0}'s state is now '{1}'.".format(job_id, state))
            logger.debug("Job {0} had issues. {1}".format(job_id, exc.message),
                         exc_info=1)
            set_status(state, build_request)
            republish(build_request, queue, channel)
            acknowledge()
        except Exception as exc:
            logger.error(exc, exc_info=1)

        # Log the artifacts...
        if artifacts:
            quote = lambda (v): "'{0}'".format(v)
            human_readable_artifacts = ', '.join([quote(a) for a in artifacts])
            logger.debug("Artifacts: {0}".format(human_readable_artifacts))

    return message_handler

def declare(queue_mappings, runner_settings, channel):
    """Declare the queue(s) and bind the pybit exchange. Also bind
    the runners to the queue declaration.

    """
    for queue, runner_name in queue_mappings.items():
        settings = runner_settings[runner_name]
        message_handler = get_message_handler(settings, queue)
        def on_queue_declared(frame):
            channel.basic_consume(message_handler, queue=queue)
        channel.queue_declare(queue=queue, durable=True, exclusive=False,
                              auto_delete=False, callback=on_queue_declared)

def on_connected(connection):
    """Called after an amqp connection has been established."""
    # Open a new channel.
    connection.channel(on_open_channel)

def on_open_channel(channel):
    """Called when a new channel has been established."""
    global config
    # Declare the queue, bind the exchange and initialize the message handlers.
    declare(config.queue_mappings, config.runners, channel)

def main(argv=None):
    """Command line utility"""
    parser = argparse.ArgumentParser(description="PyBit builder for rhaptos")
    parser.add_argument('config', type=argparse.FileType('r'),
                        help="INI configuration file")
    args = parser.parse_args(argv)

    load_logging_configuration(args.config)
    # Re-spool the file for a future read.
    args.config.seek(0)
    # Load the configuration
    global config
    config = Config.from_file(args.config)

    host = config.amqp.get('host', 'localhost')
    port = int(config.amqp.get('port', 5672))
    user = config.amqp.get('user')
    password = config.amqp.get('password')
    virtual_host = config.amqp.get('virtual_host')

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(host, port, virtual_host,
                                           credentials)
    connection = pika.SelectConnection(parameters, on_connected)

    try:
        # Loop so we can communicate with RabbitMQ
        connection.ioloop.start()
    except KeyboardInterrupt:
        # Gracefully close the connection
        connection.close()
        # Loop until we're fully closed, will stop on its own
        connection.ioloop.start()

if __name__ == '__main__':
    main()
