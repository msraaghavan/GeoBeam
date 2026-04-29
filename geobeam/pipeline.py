import apache_beam as beam

class SpatialPipeline:
    def __init__(self, options=None):
        self.pipeline = beam.Pipeline(options=options)

    def __enter__(self):
        self.pipeline.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.pipeline.__exit__(exc_type, exc_val, exc_tb)
