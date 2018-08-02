#!/usr/bin/python
"""
usage: report_haproxy.py [-h] [-c CONFIG] [-1]

Report haproxy stats to statsd

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Config file location
  -1, --once        Run once and exit

Config file format
------------------
[haproxy-statsd]
haproxy_url = http://127.0.0.1:1936/;csv
haproxy_user =
haproxy_password =
statsd_host = 127.0.0.1
statsd_port = 8125
statsd_namespace = haproxy.(HOSTNAME)
interval = 5
"""

import time
import csv
import socket
import argparse
import ConfigParser
import os

DEFAULT_SOCKET = '/var/lib/haproxy/stats'
RECV_SIZE = 1024


class HAProxySocket(object):
  def __init__(self, socket_file=DEFAULT_SOCKET):
    self.socket_file = socket_file

  def connect(self):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(self.socket_file)
    return s

  def communicate(self, command):
    ''' Send a single command to the socket and return a single response (raw string) '''
    s = self.connect()
    if not command.endswith('\n'): command += '\n'
    s.send(command)
    result = ''
    buf = ''
    buf = s.recv(RECV_SIZE)
    while buf:
      result += buf
      buf = s.recv(RECV_SIZE)
    s.close()
    return result

  def get_server_info_and_stats(self):

    output = self.communicate('show stat')
    #sanitize and make a list of lines
    output = output.lstrip('# ').strip()
    output = [ l.strip(',') for l in output.splitlines() ]
    csvreader = csv.DictReader(output)
    result = [ d.copy() for d in csvreader ]

    output = self.communicate('show info')
    for line in output.splitlines():
      try:
        key,val = line.split(':')
      except ValueError, e:
        continue
      result.append({ key.strip(): val.strip() })

    return result


def get_haproxy_report(url, user=None, password=None):
    auth = None
    if user:
        auth = HTTPBasicAuth(user, password)
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    data = r.content.lstrip('# ')
    return csv.DictReader(data.splitlines())


def is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


def report_to_statsd(stat_rows,
                     host=os.getenv('STATSD_HOST', '127.0.0.1'),
                     port=os.getenv('STATSD_PORT', 8125),
                     namespace=os.getenv('STATSD_NAMESPACE', 'haproxy')):
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    stat_count = 0

    # Report for each row
    for row in stat_rows:
        if (('pxname' in row) and ('svname' in row)):
            path = '.'.join([namespace, row['pxname'], row['svname']])
        else:
            path = '.'.join([namespace, 'general_process_info'])

        # Report each stat that we want in each row
        for stat in row:
            val = row.get(stat)

            # We skip unwanted metrics
            # (used in the path, meanignless, non numeric, or no value)
            if ((stat in ['pxname', 'svname', 'status', 'check_status',
                'check_code', 'last_chk', 'last_agt', 'pid', 'iid',
                'sid', 'tracked', 'type', 'Pid'])
                or (not is_number(val)) or (not val)):
                continue

            # By default we report a gauge.
            metric_type = 'g'

            # We report timing metrics with the proper type
            if (stat in ['check_duration', 'qtime', 'ctime', 'rtime', 'ttime']):
                metric_type = 'ms'

            stat = stat.replace('-', '_')

            udp_sock.sendto(
                '%s.%s:%s|%s' % (path, stat, val, metric_type), (host, port))
            stat_count += 1
    return stat_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Report haproxy stats to statsd')
    parser.add_argument('-c', '--config',
                        help='Config file location',
                        default='./haproxy-statsd.conf')
    parser.add_argument('-1', '--once',
                        action='store_true',
                        help='Run once and exit',
                        default=False)

    args = parser.parse_args()
    config = ConfigParser.ConfigParser({
        'haproxy_url': os.getenv('HAPROXY_HOST', 'http://127.0.0.1:1936/;csv'),
        'haproxy_socket': os.getenv('HAPROXY_SOCKET', ''),
        'haproxy_user': os.getenv('HAPROXY_USER',''),
        'haproxy_password': os.getenv('HAPROXY_PASS',''),
        'statsd_namespace': os.getenv('STATSD_NAMESPACE', 'haproxy.(HOSTNAME)'),
        'statsd_host': os.getenv('STATSD_HOST', '127.0.0.1'),
        'statsd_port': os.getenv('STATSD_PORT', 8125),
        'interval': os.getenv('HAPROXYSTATSD_INTERVAL', '5'),
    })
    config.add_section('haproxy-statsd')
    config.read(args.config)

    # Generate statsd namespace
    namespace = config.get('haproxy-statsd', 'statsd_namespace')
    if '(HOSTNAME)' in namespace:
        namespace = namespace.replace('(HOSTNAME)', socket.gethostname().replace('.', '_'))

    interval = config.getfloat('haproxy-statsd', 'interval')

    try:
        haproxy_socket = None
        socket_file = config.get('haproxy-statsd', 'haproxy_socket')
        if socket_file:
            haproxy_socket = HAProxySocket(socket_file)
        else:
            import requests
            from requests.auth import HTTPBasicAuth

        while True:
            if haproxy_socket:
                report_data = haproxy_socket.get_server_info_and_stats()
            else:
                report_data = get_haproxy_report(
                    config.get('haproxy-statsd', 'haproxy_url'),
                    user=config.get('haproxy-statsd', 'haproxy_user'),
                    password=config.get('haproxy-statsd', 'haproxy_password'))

            report_num = report_to_statsd(
                report_data,
                namespace=namespace,
                host=config.get('haproxy-statsd', 'statsd_host'),
                port=config.getint('haproxy-statsd', 'statsd_port'))

            print("Reported %s stats" % report_num)
            if args.once:
                exit(0)
            else:
                time.sleep(interval)
    except KeyboardInterrupt:
        exit(0)
