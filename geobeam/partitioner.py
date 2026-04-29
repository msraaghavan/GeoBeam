class GridPartitioner:
    kind = 'grid'

    def __init__(self, min_lon, min_lat, max_lon, max_lat, cells=10):
        self.min_lon = min_lon
        self.min_lat = min_lat
        self.max_lon = max_lon
        self.max_lat = max_lat
        self.cells = cells
        self.lon_step = (max_lon - min_lon) / cells
        self.lat_step = (max_lat - min_lat) / cells

    def cell_for(self, lon, lat):
        cx = min(max(int((lon - self.min_lon) / self.lon_step), 0), self.cells - 1)
        cy = min(max(int((lat - self.min_lat) / self.lat_step), 0), self.cells - 1)
        return (cx, cy)

    def cells_for_bbox(self, min_lon, min_lat, max_lon, max_lat):
        c1 = self.cell_for(min_lon, min_lat)
        c2 = self.cell_for(max_lon, max_lat)
        return [
            (x, y)
            for x in range(c1[0], c2[0] + 1)
            for y in range(c1[1], c2[1] + 1)
        ]


class QuadtreePartitioner:
    kind = 'quadtree'

    def __init__(self, min_lon, min_lat, max_lon, max_lat, max_depth=4):
        self.bbox = (min_lon, min_lat, max_lon, max_lat)
        self.max_depth = max_depth

    def cell_for(self, lon, lat):
        min_lon, min_lat, max_lon, max_lat = self.bbox
        path = []
        for _ in range(self.max_depth):
            mid_lon = (min_lon + max_lon) / 2
            mid_lat = (min_lat + max_lat) / 2
            if lon < mid_lon and lat < mid_lat:
                path.append(0)
                max_lon = mid_lon
                max_lat = mid_lat
            elif lon < mid_lon:
                path.append(1)
                max_lon = mid_lon
                min_lat = mid_lat
            elif lat < mid_lat:
                path.append(2)
                min_lon = mid_lon
                max_lat = mid_lat
            else:
                path.append(3)
                min_lon = mid_lon
                min_lat = mid_lat
        return tuple(path)

    def cells_for_bbox(self, qx1, qy1, qx2, qy2):
        cells = []
        self._walk(self.bbox, [], 0, qx1, qy1, qx2, qy2, cells)
        return cells

    def _walk(self, bbox, path, depth, qx1, qy1, qx2, qy2, cells):
        min_lon, min_lat, max_lon, max_lat = bbox
        if max_lon < qx1 or min_lon > qx2 or max_lat < qy1 or min_lat > qy2:
            return
        if depth == self.max_depth:
            cells.append(tuple(path))
            return
        mid_lon = (min_lon + max_lon) / 2
        mid_lat = (min_lat + max_lat) / 2
        children = [
            (0, (min_lon, min_lat, mid_lon, mid_lat)),
            (1, (min_lon, mid_lat, mid_lon, max_lat)),
            (2, (mid_lon, min_lat, max_lon, mid_lat)),
            (3, (mid_lon, mid_lat, max_lon, max_lat)),
        ]
        for quadrant, child_bbox in children:
            self._walk(child_bbox, path + [quadrant], depth + 1, qx1, qy1, qx2, qy2, cells)


def make_partitioner(kind, min_lon, min_lat, max_lon, max_lat, cells=10, max_depth=4):
    if kind == 'quadtree':
        return QuadtreePartitioner(
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            max_depth=max_depth,
        )
    return GridPartitioner(min_lon, min_lat, max_lon, max_lat, cells=cells)
