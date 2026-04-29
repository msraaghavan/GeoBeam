import argparse
import json
import os

import apache_beam as beam

from geobeam.combiners import CountPerCell
from geobeam.config import GeoBeamConfig
from geobeam.metrics import CountedDoFn, collect_metrics
from geobeam.partitioner import make_partitioner
from geobeam.pcollection import SpatialPCollection
from geobeam.pipeline import SpatialPipeline
from geobeam.sampler import AdaptiveGridPartitioner, sample_input
from geobeam.transforms import KNN, RangeQuery, SpatialJoin


def parse_row(line):
    parts = line.split(',')
    return {
        'trip_id': parts[0],
        'lon': float(parts[1]),
        'lat': float(parts[2]),
        'fare': float(parts[3]),
    }


def load_geofences():
    with open('data/geofences.geojson') as handle:
        return json.load(handle)['features']


def output_base(op_name):
    return 'data/output_{0}'.format(op_name)


def output_file(op_name):
    return 'data/output_{0}.tsv'.format(op_name)


def clean_output(op_name):
    path = output_file(op_name)
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def write_lines(pcoll, formatter, base_path):
    return (
        pcoll
        | 'FormatOutput' >> beam.Map(formatter)
        | 'WriteOutput' >> beam.io.WriteToText(
            base_path,
            file_name_suffix='.tsv',
            shard_name_template='',
        )
    )


def build_points_pipeline(options, label_suffix):
    spatial_pipeline = SpatialPipeline(options=options)
    points = SpatialPCollection(
        spatial_pipeline.pipeline
        | f'Read{label_suffix}' >> beam.io.ReadFromText('data/taxi_sample.csv', skip_header_lines=1)
        | f'Parse{label_suffix}' >> beam.Map(parse_row)
        | f'Meter{label_suffix}' >> beam.ParDo(CountedDoFn()),
        geom_field='point',
    )
    return spatial_pipeline, points


def format_trip(record):
    return '{0}\t{1}\t{2}\t{3}'.format(
        record['trip_id'],
        record['lon'],
        record['lat'],
        record['fare'],
    )


def format_join(record):
    return '{0}\t{1}\t{2}\t{3}\t{4}'.format(
        record['trip_id'],
        record['lon'],
        record['lat'],
        record['fare'],
        record['zone'],
    )


def format_count(item):
    cell, count = item
    return '{0}\t{1}'.format(':'.join(str(part) for part in cell), count)


def run_range(args, options, partitioner):
    bbox = (args.min_lon, args.min_lat, args.max_lon, args.max_lat)
    spatial_pipeline, points = build_points_pipeline(options, 'Range')
    result = points.pcoll | 'RangeQuery' >> RangeQuery(partitioner, bbox)
    write_lines(result, format_trip, output_base('range'))
    pipeline_result = spatial_pipeline.pipeline.run()
    pipeline_result.wait_until_finish()
    return collect_metrics(pipeline_result)


def run_join(_args, options, _partitioner):
    spatial_pipeline, points = build_points_pipeline(options, 'Join')
    fences = spatial_pipeline.pipeline | 'ReadFences' >> beam.Create(load_geofences())
    fences_side = beam.pvalue.AsList(fences)
    result = points.pcoll | 'SpatialJoin' >> SpatialJoin(fences_side)
    write_lines(result, format_join, output_base('join'))
    pipeline_result = spatial_pipeline.pipeline.run()
    pipeline_result.wait_until_finish()
    return collect_metrics(pipeline_result)


def run_knn(args, options, partitioner):
    query_point = (args.lon, args.lat)
    spatial_pipeline, points = build_points_pipeline(options, 'KNN')
    result = points.pcoll | 'KNN' >> KNN(partitioner, query_point, k=args.k)
    write_lines(result, format_trip, output_base('knn'))
    pipeline_result = spatial_pipeline.pipeline.run()
    pipeline_result.wait_until_finish()
    return collect_metrics(pipeline_result)


def run_stats(_args, options, partitioner):
    spatial_pipeline, points = build_points_pipeline(options, 'Stats')
    result = points.pcoll | 'CountPerCell' >> CountPerCell(partitioner)
    write_lines(result, format_count, output_base('stats'))
    pipeline_result = spatial_pipeline.pipeline.run()
    pipeline_result.wait_until_finish()
    return collect_metrics(pipeline_result)


def print_metrics(metrics):
    for key in sorted(metrics):
        print('METRIC {0}={1}'.format(key, metrics[key]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--op', required=True, choices=['range', 'join', 'knn', 'stats'])
    parser.add_argument('--min_lon', type=float)
    parser.add_argument('--min_lat', type=float)
    parser.add_argument('--max_lon', type=float)
    parser.add_argument('--max_lat', type=float)
    parser.add_argument('--lon', type=float)
    parser.add_argument('--lat', type=float)
    parser.add_argument('--k', type=int, default=10)
    parser.add_argument('--partitioner', default='grid', choices=['grid', 'quadtree', 'adaptive'])
    parser.add_argument('--runner', default='DirectRunner')
    args = parser.parse_args()

    clean_output(args.op)
    config = GeoBeamConfig(runner=args.runner, partitioner=args.partitioner, cells=10)
    print('CONFIG {0}'.format(config.describe()))

    if config.partitioner == 'adaptive':
        samples = sample_input('data/taxi_sample.csv', rate=0.05)
        partitioner = AdaptiveGridPartitioner(samples, target_cells=10)
        print('INPUT sampled_points={0}'.format(len(samples)))
    else:
        partitioner = make_partitioner(
            config.partitioner,
            -74.05,
            40.65,
            -73.85,
            40.85,
            cells=config.cells,
            max_depth=config.max_depth,
        )

    options = config.to_pipeline_options()

    if args.op == 'range':
        metrics = run_range(args, options, partitioner)
    elif args.op == 'join':
        metrics = run_join(args, options, partitioner)
    elif args.op == 'knn':
        metrics = run_knn(args, options, partitioner)
    else:
        metrics = run_stats(args, options, partitioner)

    print_metrics(metrics)
    print('OUTPUT {0}'.format(output_file(args.op)))


if __name__ == '__main__':
    main()
