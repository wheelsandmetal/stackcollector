import contextlib
import click
# noinspection PyCompatibility
import dbm
import logging
import requests
import time


DEFAULT_STACKCOLLECTOR_DATA_DIR = '/var/cb/data/stackcollector'

_logger = logging.getLogger(__name__)


@contextlib.contextmanager
def getdb(dbpath):
    handle = None
    while True:
        try:
            handle = dbm.ndbm.open(dbpath, 'c')
            break
        except dbm.ndbm.error as exc:
            if exc.args[0] == 11:
                continue
            else:
                raise
    try:
        yield handle
    finally:
        handle.close()


def collect(dbpath, host, port):
    try:
        resp = requests.get('http://{}:{}?reset=true'.format(host, port))
        resp.raise_for_status()
    # except (requests.ConnectionError, requests.HTTPError):
    except Exception:
        _logger.exception('Error collecting data ({}:{})'.format(host, port))
        return
    data = resp.content.splitlines()

    try:
        save(data, host, port, dbpath)
    except Exception:
        _logger.exception('Error saving data ({}:{})'.format(host, port))
        return
    _logger.info('Data collected: {}:{}, num_stack: {}'.format(host, port, len(data) - 2))


def save(data, host, port, dbpath):
    now = int(time.time())
    with getdb(dbpath) as db:
        for line in data[2:]:
            try:
                line = line.decode('utf-8')
                stack, value = line.split()
            except ValueError:
                continue

            try:
                entry = '{}:{}:{}:{} '.format(host, port, now, value).encode('utf-8')
                if stack in db:
                    db[stack] += entry
                else:
                    db[stack] = entry
            except Exception:
                _logger.exception("Error saving data")


@click.command()
@click.option('--dbpath', '-d', default=DEFAULT_STACKCOLLECTOR_DATA_DIR)
@click.option('--host', '-h', multiple=True)
@click.option('--ports', '-p')
@click.option('--interval', '-i', type=int, default=600)
def run(dbpath, host, ports, interval):
    # TODO(emfree) document port format; handle parsing errors
    if '..' in ports:
        start, end = ports.split('..')
        start = int(start)
        end = int(end)
        ports = list(range(start, end + 1))
    elif ',' in ports:
        ports = [int(p) for p in ports.split(',')]
    else:
        ports = [int(ports)]
    while True:
        for h in host:
            for port in ports:
                collect(dbpath, h, port)
        time.sleep(interval)


if __name__ == '__main__':
    run()
