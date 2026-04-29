import json
import os
import subprocess
import sys

from flask import Flask, Response, render_template, request


app = Flask(__name__)


def run_subprocess(args):
    return subprocess.run([sys.executable, 'run_pipeline.py'] + args, capture_output=True, text=True)


def parse_metrics(stdout):
    metrics = []
    for line in stdout.splitlines():
        if line.startswith('METRIC '):
            metrics.append(line[len('METRIC '):].strip())
    return '; '.join(metrics) if metrics else 'none'


def output_file(mode):
    return 'data/output_{0}.tsv'.format(mode)


def read_output_lines(mode):
    path = output_file(mode)
    if not os.path.exists(path):
        return []
    with open(path) as handle:
        return [line.strip() for line in handle if line.strip()]


def make_plain_response(mode, stdout):
    lines = read_output_lines(mode)
    payload = [
        'MODE\t{0}'.format(mode),
        'COUNT\t{0}'.format(len(lines)),
        'METRICS\t{0}'.format(parse_metrics(stdout)),
    ]
    payload.extend(lines)
    return Response('\n'.join(payload), mimetype='text/plain')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data/pickups.json')
def pickups():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'pickups.json')
    with open(path) as handle:
        return Response(handle.read(), mimetype='application/json')


@app.route('/api/range', methods=['POST'])
def api_range():
    data = request.get_json()
    bbox = data['bbox']
    result = run_subprocess([
        '--op', 'range',
        '--min_lon', str(bbox[0]),
        '--min_lat', str(bbox[1]),
        '--max_lon', str(bbox[2]),
        '--max_lat', str(bbox[3]),
        '--partitioner', data.get('partitioner', 'grid'),
    ])
    if result.returncode != 0:
        return Response(result.stderr, status=500, mimetype='text/plain')
    return make_plain_response('range', result.stdout)


@app.route('/api/join', methods=['POST'])
def api_join():
    data = request.get_json() or {}
    result = run_subprocess([
        '--op', 'join',
        '--partitioner', data.get('partitioner', 'grid'),
    ])
    if result.returncode != 0:
        return Response(result.stderr, status=500, mimetype='text/plain')
    return make_plain_response('join', result.stdout)


@app.route('/api/knn', methods=['POST'])
def api_knn():
    data = request.get_json()
    result = run_subprocess([
        '--op', 'knn',
        '--lon', str(data['lon']),
        '--lat', str(data['lat']),
        '--k', str(data.get('k', 10)),
        '--partitioner', data.get('partitioner', 'grid'),
    ])
    if result.returncode != 0:
        return Response(result.stderr, status=500, mimetype='text/plain')
    return make_plain_response('knn', result.stdout)


@app.route('/api/stats', methods=['POST'])
def api_stats():
    data = request.get_json() or {}
    result = run_subprocess([
        '--op', 'stats',
        '--partitioner', data.get('partitioner', 'grid'),
    ])
    if result.returncode != 0:
        return Response(result.stderr, status=500, mimetype='text/plain')
    return make_plain_response('stats', result.stdout)


@app.route('/api/geofences', methods=['GET'])
def api_geofences():
    with open('data/geofences.geojson') as handle:
        geofences = json.load(handle)['features']
    lines = []
    for geofence in geofences:
        name = geofence['properties']['name']
        coords = geofence['geometry']['coordinates'][0]
        points = ';'.join('{0},{1}'.format(lon, lat) for lon, lat in coords)
        lines.append('{0}\t{1}'.format(name, points))
    return Response('\n'.join(lines), mimetype='text/plain')


if __name__ == '__main__':
    app.run(debug=True, port=5000)
