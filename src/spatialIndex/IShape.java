package spatialIndex;

public interface IShape {
    public boolean intersects(final IShape s);
    public boolean contains(final IShape s);
    public boolean touches(final IShape s);
    public double[] getCenter();
    public long getDimension();
    public Region getMBR();
    public double getArea();
    public double getMinimumDistance(final IShape s);
}
