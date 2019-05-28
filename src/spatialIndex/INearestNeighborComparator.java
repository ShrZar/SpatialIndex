package spatialIndex;

public interface INearestNeighborComparator {
    public double getMinimumDistance(IShape query, IEntry e);
}
