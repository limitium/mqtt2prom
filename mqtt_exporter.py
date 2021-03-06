#!/usr/bin/env python

import prometheus_client as prometheus
from collections import defaultdict
import logging
import argparse
import paho.mqtt.client as mqtt
import yaml
import os
import re
import operator
import time
import signal
import sys
from yamlreader import yaml_load
from prometheus_client.metrics_core import METRIC_LABEL_NAME_RE

VERSION = '0.1'


def _read_config(config_path):
    """Read config file from given location, and parse properties"""

    if config_path is not None:
        if os.path.isfile(config_path):
            logging.info(f'Config file found at: {config_path}')
            try:
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f.read())
            except yaml.YAMLError:
                logging.exception('Failed to parse configuration file:')

        elif os.path.isdir(config_path):
            logging.info(
                f'Config directory found at: {config_path}')
            try:
                return yaml_load(config_path)
            except yaml.YAMLError:
                logging.exception('Failed to parse configuration directory:')

    return {}


def _parse_config_and_add_defaults(config_from_file):
    """Parse content of configfile and add default values where needed"""

    config = {}
    logging.debug(f'_parse_config Config from file: {str(config_from_file)}')
    # Logging values ('logging' is optional in config
    if 'logging' in config_from_file:
        config['logging'] = _add_config_and_defaults(
            config_from_file['logging'], {'logfile': '', 'level': 'info'})
    else:
        config['logging'] = _add_config_and_defaults(
            None, {'logfile': '', 'level': 'info'})

    # MQTT values
    if 'mqtt' in config_from_file:
        config['mqtt'] = _add_config_and_defaults(
            config_from_file['mqtt'], {'host': 'localhost'})
    else:
        config['mqtt'] = _add_config_and_defaults(None, {'host': 'localhost'})

    if 'auth' in config['mqtt']:
        config['mqtt']['auth'] = _add_config_and_defaults(
            config['mqtt']['auth'], {})
        _validate_required_fields(config['mqtt']['auth'], 'auth', ['username'])

    if 'tls' in config['mqtt']:
        config['mqtt']['tls'] = _add_config_and_defaults(
            config['mqtt']['tls'], {})

    # Prometheus values
    if 'prometheus' in config:
        config['prometheus'] = _add_config_and_defaults(
            config_from_file['prometheus'], {'exporter_port': 9344})
    else:
        config['prometheus'] = _add_config_and_defaults(
            None, {'exporter_port': 9344})

    return config


def _validate_required_fields(config, parent, required_fields):
    """Fail if required_fields is not present in config"""
    for field in required_fields:
        if field not in config or config[field] is None:
            if parent is None:
                error = f'\'{field}\' is a required field in configfile'
            else:
                error = f'\'{field}\' is a required parameter for field {parent} in configfile'
            raise TypeError(error)


def _add_config_and_defaults(config, defaults):
    """Return dict with values from config, if present, or values from defaults"""
    if config is not None:
        defaults.update(config)
    return defaults.copy()


def _strip_config(config, allowed_keys):
    return {k: v for k, v in config.items() if k in allowed_keys and v}


# noinspection SpellCheckingInspection
def _log_setup(logging_config):
    """Setup application logging"""

    logfile = logging_config['logfile']

    log_level = logging_config['level']

    numeric_level = logging.getLevelName(log_level.upper())
    if not isinstance(numeric_level, int):
        raise TypeError(f'Invalid log level: {log_level}')

    if logfile != '':
        logging.info('Logging redirected to: ' + logfile)
        # Need to replace the current handler on the root logger:
        file_handler = logging.FileHandler(logfile, 'a')
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        file_handler.setFormatter(formatter)

        log = logging.getLogger()  # root logger
        for handler in log.handlers:  # remove all old handlers
            log.removeHandler(handler)
        log.addHandler(file_handler)

    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

    logging.getLogger().setLevel(numeric_level)
    logging.info(f'log_level set to: {log_level}')


# noinspection PyUnusedLocal
def _on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK response from the server."""
    logging.info(f'Connected to broker, result code {str(rc)}')
    topic = userdata['topic']
    client.subscribe(topic)
    logging.info(f'Subscribing to topic: {topic}')


# noinspection PyUnusedLocal
def _on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    payload = msg.payload
    logging.debug(
        f'_on_message Msg received on topic: {msg.topic}, Value: {str(payload)}')
    path = msg.topic.split('/')
    if len(path) != 7:
        return
    (space, bloomy, v1, user, zone, sensor, param) = path
    user = user[:6]
    zone = zone[:16]
    sensor = sensor[:16]
    param = param[:16]
    
    label = user + '_' + zone + '_' + sensor + '_' + param
    if not METRIC_LABEL_NAME_RE.match(label):
        logging.debug(f'Wrong label "{label}"')
        return
    try:
        float(payload)
    except ValueError:
        logging.critical(f'Payload isn\' float {payload}')

    _export_to_prometheus(userdata['metrics']['users'], label, payload)
    
    clean_up_orphan_metrics(userdata['metrics']['users'], userdata['metric_ttl'])
    
    add_exporter_metrics(userdata['metrics'])


def _mqtt_init(mqtt_config):
    """Setup mqtt connection"""
    logging.info(f'Connecting {str(mqtt_config)}')
    mqtt_client = mqtt.Client(userdata={
        'topic': mqtt_config['topic'],
        'metric_ttl': mqtt_config['metric_ttl'],
        'metrics': {
            'exporter': {},
            'users': {}
        }
    })
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_message = _on_message

    if 'auth' in mqtt_config:
        auth = _strip_config(mqtt_config['auth'], ['username', 'password'])
        mqtt_client.username_pw_set(**auth)

    if 'tls' in mqtt_config:
        tls_config = _strip_config(mqtt_config['tls'], [
                                   'ca_certs', 'certfile', 'keyfile', 'cert_reqs', 'tls_version'])
        mqtt_client.tls_set(**tls_config)

    mqtt_client.connect(**_strip_config(mqtt_config,
                                        ['host', 'port', 'keepalive']))
    return mqtt_client


def _export_to_prometheus(metrics, label, value):
    """Export metric and labels to prometheus."""
    try:
        gauge(metrics, label, value)
    except ValueError as err:
        logging.critical(f'Unable to update metric {str(err)}')
    logging.debug(
        f'_export_to_prometheus metric {label} updated with value: {value}')


def gauge(metrics, label, value):
    """Define metric as Gauge, setting it to 'value'"""
    get_prometheus_metric(metrics, label).set(value)


def get_prometheus_metric(metrics, label):
    key = label
    if key not in metrics:
        metrics[key] = {'metric':prometheus.Gauge(label, ''), 'updated':time.time()}
        logging.debug(f'get_prometheus_metric create new metric{label}')
    metrics[key]['updated'] = time.time()
    return metrics[key]['metric']

def clean_up_orphan_metrics(metrics, ttl):
    for key in list(metrics.keys()):
        if time.time() - metrics[key]['updated'] > ttl:
            prometheus.REGISTRY.unregister(metrics[key]['metric'])
            del metrics[key]
            logging.debug(f'clean_up_orphan_metrics remove metric{key}')


def add_exporter_metrics(metrics):
    if 'memory' not in metrics['exporter']:
        metrics['exporter']['memory'] = prometheus.Gauge(
            'mqtt_exporter_usage_memory_mb', 'Memory usage')
    metrics['exporter']['memory'].set(getCurrentMemoryUsage())

    if 'metrics_total' not in metrics['exporter']:
        metrics['exporter']['metrics_total'] = prometheus.Gauge(
            'mqtt_exporter_metrics_total', 'Total metrics')
    metrics['exporter']['metrics_total'].set(len(metrics['users'].keys()))


def add_static_metric(timestamp):
    g = prometheus.Gauge('mqtt_exporter_timestamp', 'Startup time of exporter in millis since EPOC (static)',
                         ['exporter_version'])
    g.labels(VERSION).set(timestamp)


def _get_sorted_tuple_list(source):
    """Return a sorted list of tuples"""
    filtered_source = source.copy()
    sorted_tuple_list = sorted(
        list(filtered_source.items()), key=operator.itemgetter(0))
    return sorted_tuple_list


def _signal_handler(sig, frame):
    # pylint: disable=E1101
    logging.info('Received {0}'.format(signal.Signals(sig).name))
    sys.exit(0)


def getCurrentMemoryUsage():
    return 1


def main():
    add_static_metric(int(time.time() * 1000))
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Simple program to export formatted mqtt messages to prometheus')
    parser.add_argument('-c', '--config', action='store', dest='config', default='conf',
                        help='Set config location (file or directory), default: \'conf\'')
    options = parser.parse_args()

    # Initial logging to console
    _log_setup({'logfile': '', 'level': 'info'})
    signal.signal(signal.SIGINT, _signal_handler)

    # Read config file from disk
    from_file = _read_config(options.config)
    config = _parse_config_and_add_defaults(from_file)

    # Set up logging
    _log_setup(config['logging'])

    # Start prometheus exporter
    logging.info(
        f"Starting prometheus exporter on port: {str(config['prometheus']['exporter_port'])}")
    prometheus.start_http_server(config['prometheus']['exporter_port'])

    # Set up mqtt client and loop forever
    mqtt_client = _mqtt_init(config['mqtt'])
    mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
