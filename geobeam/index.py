from shapely.strtree import STRtree


class SpatialIndex:
    def __init__(self, geometries):
        self.geometries = list(geometries)
        self.tree = STRtree(self.geometries) if self.geometries else None

    def query(self, geom):
        if self.tree is None:
            return []
        return [int(i) for i in self.tree.query(geom)]


class IndexManager:
    def __init__(self, records):
        from shapely.geometry import Point

        self.records = list(records)
        self.geometries = [Point(record['lon'], record['lat']) for record in self.records]
        self.index = SpatialIndex(self.geometries)

    def range_query(self, query_bbox):
        from shapely.geometry import box

        query_geom = box(*query_bbox)
        return [self.records[i] for i in self.index.query(query_geom)]
