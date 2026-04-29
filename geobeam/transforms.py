import apache_beam as beam


class AssignCell(beam.DoFn):
    def __init__(self, partitioner):
        self.partitioner = partitioner

    def process(self, record):
        yield (self.partitioner.cell_for(record['lon'], record['lat']), record)


class RangeQueryPerCell(beam.DoFn):
    def __init__(self, query_bbox):
        self.query_bbox = query_bbox

    def process(self, element):
        from geobeam.index import IndexManager

        _, records = element
        records = list(records)
        if not records:
            return
        manager = IndexManager(records)
        for record in manager.range_query(self.query_bbox):
            yield record


class RangeQuery(beam.PTransform):
    def __init__(self, partitioner, query_bbox):
        super().__init__()
        self.partitioner = partitioner
        self.query_bbox = query_bbox

    def expand(self, pcoll):
        candidates = set(self.partitioner.cells_for_bbox(*self.query_bbox))
        return (
            pcoll
            | 'AssignCell' >> beam.ParDo(AssignCell(self.partitioner))
            | 'FilterCells' >> beam.Filter(lambda kv, cells: kv[0] in cells, candidates)
            | 'GroupByCell' >> beam.GroupByKey()
            | 'QueryPerCell' >> beam.ParDo(RangeQueryPerCell(self.query_bbox))
        )


class SpatialJoinDoFn(beam.DoFn):
    def process(self, record, geofences):
        from shapely.geometry import Point, shape

        point = Point(record['lon'], record['lat'])
        for fence in geofences:
            if shape(fence['geometry']).contains(point):
                output = dict(record)
                output['zone'] = fence['properties']['name']
                yield output
                return
        output = dict(record)
        output['zone'] = 'OUTSIDE'
        yield output


class SpatialJoin(beam.PTransform):
    def __init__(self, geofences_side):
        super().__init__()
        self.geofences_side = geofences_side

    def expand(self, pcoll):
        return pcoll | 'JoinPoints' >> beam.ParDo(SpatialJoinDoFn(), geofences=self.geofences_side)


class LocalKNN(beam.DoFn):
    def __init__(self, k):
        self.k = k

    def process(self, element, query_point):
        _, records = element
        records = list(records)
        if not records:
            return
        qx, qy = query_point
        scored = []
        for record in records:
            dx = record['lon'] - qx
            dy = record['lat'] - qy
            scored.append((dx * dx + dy * dy, record))
        scored.sort(key=lambda item: item[0])
        for distance, record in scored[:self.k]:
            yield (distance, record)


class KNN(beam.PTransform):
    def __init__(self, partitioner, query_point, k=5):
        super().__init__()
        self.partitioner = partitioner
        self.query_point = query_point
        self.k = k

    def expand(self, pcoll):
        k = self.k
        return (
            pcoll
            | 'Assign' >> beam.ParDo(AssignCell(self.partitioner))
            | 'Group' >> beam.GroupByKey()
            | 'LocalTopK' >> beam.ParDo(LocalKNN(k * 2), query_point=self.query_point)
            | 'GlobalKey' >> beam.Map(lambda item: (None, item))
            | 'GlobalGroup' >> beam.GroupByKey()
            | 'GlobalTopK' >> beam.FlatMap(lambda item: sorted(list(item[1]), key=lambda pair: pair[0])[:k])
            | 'Strip' >> beam.Map(lambda item: item[1])
        )
