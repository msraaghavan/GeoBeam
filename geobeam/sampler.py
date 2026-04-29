import random


class AdaptiveGridPartitioner:
    kind = 'adaptive'

    def __init__(self, samples, target_cells=10):
        lons = sorted(sample[0] for sample in samples)
        lats = sorted(sample[1] for sample in samples)
        count = len(lons)
        if count < target_cells:
            self.lon_breaks = [lons[0], lons[-1]]
            self.lat_breaks = [lats[0], lats[-1]]
        else:
            step = count // target_cells
            self.lon_breaks = [lons[i * step] for i in range(target_cells)] + [lons[-1]]
            self.lat_breaks = [lats[i * step] for i in range(target_cells)] + [lats[-1]]
        self.cells = target_cells

    def _bisect(self, breaks, value):
        for i in range(len(breaks) - 1):
            if value <= breaks[i + 1]:
                return i
        return len(breaks) - 2

    def cell_for(self, lon, lat):
        return (self._bisect(self.lon_breaks, lon), self._bisect(self.lat_breaks, lat))

    def cells_for_bbox(self, qx1, qy1, qx2, qy2):
        x1 = self._bisect(self.lon_breaks, qx1)
        x2 = self._bisect(self.lon_breaks, qx2)
        y1 = self._bisect(self.lat_breaks, qy1)
        y2 = self._bisect(self.lat_breaks, qy2)
        return [(x, y) for x in range(x1, x2 + 1) for y in range(y1, y2 + 1)]


def sample_input(csv_path, rate=0.05, seed=42):
    randomizer = random.Random(seed)
    samples = []
    with open(csv_path) as handle:
        next(handle)
        for line in handle:
            if randomizer.random() < rate:
                parts = line.split(',')
                samples.append((float(parts[1]), float(parts[2])))
    return samples
