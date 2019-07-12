import calendar
import click
import dateparser
from flask import Flask, request, jsonify
import logging

from .collector import getdb, DEFAULT_STACKCOLLECTOR_DATA_DIR

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9999

_logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['DEBUG'] = True


def _parse_relative_date(datestr):
    return calendar.timegm(dateparser.parse(datestr).utctimetuple())


class Node(object):
    def __init__(self, name):
        self.name = name
        self.value = 0
        self.children = {}

    def serialize(self, threshold=None):
        res = {
            'name': self.name,
            'value': self.value
        }
        if self.children:
            serialized_children = [
                child.serialize(threshold)
                for _, child in sorted(self.children.items())
                if child.value > threshold
            ]
            if serialized_children:
                res['children'] = serialized_children
        return res

    def add(self, frames, value):
        self.value += value
        if not frames:
            return
        head = frames[0]
        child = self.children.get(head)
        if child is None:
            child = Node(name=head)
            self.children[head] = child
        child.add(frames[1:], value)

    def add_raw(self, line):
        frames, value = line.split()
        frames = frames.split(';')
        try:
            value = int(value)
        except ValueError:
            return
        self.add(frames, value)


@app.route('/data')
def data():
    from_ = request.args.get('from')
    if from_ is not None:
        from_ = _parse_relative_date(from_)
    until = request.args.get('until')
    if until is not None:
        until = _parse_relative_date(until)
    threshold = float(request.args.get('threshold', 0))
    root = Node('root')

    with getdb(app.config['DBPATH']) as db:
        keys = list(db.keys())
        for k in keys:
            entries = db[k].split()
            value = 0
            for e in entries:
                try:
                    e = e.decode('utf-8')
                    host, port, ts, v = e.split(':')
                    ts = int(ts)
                    v = int(v)
                    if ((from_ is None or ts >= from_) and
                            (until is None or ts <= until)):
                        value += v
                except ValueError as ex:
                    continue
            frames = k.decode('utf-8').split(';')
            root.add(frames, value)

    return jsonify(root.serialize(threshold * root.value))


@app.route('/')
def render():
    return app.send_static_file('index.html')


@click.command()
@click.option('--port', type=int, default=DEFAULT_PORT)
@click.option('--dbpath', '-d', default=DEFAULT_STACKCOLLECTOR_DATA_DIR)
def run(port, dbpath):
    print("*** Visualizer running at {}:{} , reading data from {} ***".format(DEFAULT_HOST, port, dbpath))
    app.config['DBPATH'] = dbpath
    app.run(host=DEFAULT_HOST, port=port)


if __name__ == '__main__':
    run()
