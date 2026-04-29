import time

import apache_beam as beam
from apache_beam.metrics import Metrics


class CountedDoFn(beam.DoFn):
    def __init__(self, namespace='geobeam'):
        self.records_in = Metrics.counter(namespace, 'records_in')
        self.records_out = Metrics.counter(namespace, 'records_out')
        self.latency = Metrics.distribution(namespace, 'process_latency_us')

    def process(self, element):
        start = time.time()
        self.records_in.inc()
        yield element
        self.records_out.inc()
        self.latency.update(int((time.time() - start) * 1000000))


def collect_metrics(result):
    metrics = {}
    query = result.metrics().query()
    for counter in query.get('counters', []):
        metrics[counter.key.metric.name] = str(counter.committed)
    for distribution in query.get('distributions', []):
        committed = distribution.committed
        if committed.count:
            metrics[distribution.key.metric.name] = 'count={0},mean={1},min={2},max={3}'.format(
                committed.count,
                committed.mean,
                committed.min,
                committed.max,
            )
    return metrics
