class SpatialPCollection:
    def __init__(self, pcoll, geom_field='geometry'):
        self.pcoll = pcoll
        self.geom_field = geom_field
