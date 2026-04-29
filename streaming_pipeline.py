import apache_beam as beam
from apache_beam.transforms.window import FixedWindows


def parse_with_timestamp(line):
    parts = line.split(',')
    record = {
        'trip_id': parts[0],
        'lon': float(parts[1]),
        'lat': float(parts[2]),
        'fare': float(parts[3]),
        'event_time': int(parts[4]),
    }
    return beam.window.TimestampedValue(record, record['event_time'])


def format_window(item, window=beam.DoFn.WindowParam):
    return '{0}\t{1}\t{2}'.format(int(window.start), int(window.end), item[1])


def main():
    options = beam.options.pipeline_options.PipelineOptions(['--runner=DirectRunner'])
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadStream' >> beam.io.ReadFromText('data/taxi_stream.csv', skip_header_lines=1)
            | 'Stamp' >> beam.Map(parse_with_timestamp)
            | 'Window' >> beam.WindowInto(FixedWindows(10))
            | 'Key' >> beam.Map(lambda record: (None, record))
            | 'CountPerWindow' >> beam.combiners.Count.PerKey()
            | 'FormatWindow' >> beam.Map(format_window)
            | 'WriteStream' >> beam.io.WriteToText(
                'data/stream_output',
                file_name_suffix='.tsv',
                shard_name_template='',
            )
        )


if __name__ == '__main__':
    main()
