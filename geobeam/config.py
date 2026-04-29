from apache_beam.options.pipeline_options import PipelineOptions


class GeoBeamConfig:
    def __init__(self, runner='DirectRunner', partitioner='grid', cells=10, max_depth=4):
        self.runner = runner
        self.partitioner = partitioner
        self.cells = cells
        self.max_depth = max_depth

    def to_pipeline_options(self):
        return PipelineOptions(['--runner=' + self.runner])

    def describe(self):
        return 'runner={0} partitioner={1} cells={2}'.format(
            self.runner,
            self.partitioner,
            self.cells,
        )
