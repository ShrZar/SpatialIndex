package spatialIndex;

public class Point implements IShape,Cloneable{
    public double[] pCoords=null;
    public byte[] pData=null;
    public Point(double[] pCoords){
        this.pCoords=new double[pCoords.length];
        System.arraycopy(pCoords, 0, this.pCoords, 0, pCoords.length);
    }
    public Point(double[] pCoords,byte[] pData){
        this.pCoords=new double[pCoords.length];
        this.pData=new byte[pData.length];
        System.arraycopy(pCoords, 0, this.pCoords, 0, pCoords.length);
        System.arraycopy(pData, 0, this.pData, 0, pData.length);
    }
    public Point(final Point pt)
    {
        this.pCoords = new double[pt.pCoords.length];
        this.pData = new byte[pt.pData.length];
        System.arraycopy(pt.pCoords, 0, this.pCoords, 0, pt.pCoords.length);
        System.arraycopy(pt.pData, 0, this.pData, 0, pt.pData.length);
    }

    public boolean equals(Object o)
    {
        if (o instanceof Point)
        {
            Point pt = (Point) o;

            if (pt.pCoords.length != this.pCoords.length) return false;

            for (int cIndex = 0; cIndex < this.pCoords.length; cIndex++)
            {
                if (this.pCoords[cIndex] < pt.pCoords[cIndex] - SpatialIndex.EPSILON ||
                        this.pCoords[cIndex] > pt.pCoords[cIndex] + SpatialIndex.EPSILON) return false;
            }
            return true;
        }
        return false;
    }
    public Object clone()
    {
        return new Point(this.pCoords);
    }
    public boolean intersects(final IShape s)
    {
        if (s instanceof Region) return ((Region) s).contains(this);
        return false;
    }
    public boolean contains(final IShape s)
    {
        return false;
    }
    public boolean touches(final IShape s)
    {
        if (s instanceof Point && this.equals(s)) return true;

        if (s instanceof Region) return ((Region) s).touches(this);

        return false;
    }
    public double[] getCenter()
    {
        double[] pCoords = new double[this.pCoords.length];
        System.arraycopy(this.pCoords, 0, pCoords, 0, this.pCoords.length);
        return pCoords;
    }
    public long getDimension()
    {
        return this.pCoords.length;
    }
    public Region getMBR()
    {
        return new Region(this.pCoords, this.pCoords);
    }
    public double getArea()
    {
        return 0.0f;
    }
    public double getMinimumDistance(final IShape s)
    {
        if (s instanceof Region) return ((Region) s).getMinimumDistance(this);

        if (s instanceof Point) return getMinimumDistance((Point) s);

        throw new IllegalStateException("getMinimumDistance: Not implemented yet!");
    }
    double getMinimumDistance(final Point p)
    {
        if (this.pCoords.length != p.pCoords.length) throw new IllegalArgumentException("getMinimumDistance: Shape has the wrong number of dimensions.");

        double ret = 0.0;

        for (int cIndex = 0; cIndex < this.pCoords.length; cIndex++)
        {
            ret += Math.pow(this.pCoords[cIndex] - p.pCoords[cIndex], 2.0);
        }
        return Math.sqrt(ret);
    }
    public double getCoord(int index) throws IndexOutOfBoundsException
    {
        if (index >= this.pCoords.length) throw new IndexOutOfBoundsException("" + index);
        return this.pCoords[index];
    }
}
