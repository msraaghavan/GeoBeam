import apache_beam as beam

from .transforms import AssignCell


class CountPerCell(beam.PTransform):
    def __init__(self, partitioner):
        super().__init__()
        self.partitioner = partitioner

    def expand(self, pcoll):
        return (
            pcoll
            | 'AssignForCount' >> beam.ParDo(AssignCell(self.partitioner))
            | 'CountPerKey' >> beam.combiners.Count.PerKey()
        )
