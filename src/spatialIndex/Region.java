package spatialIndex;

import java.util.HashMap;

public class Region implements IShape{
    public double[] pLow = null;
    public double[] pHigh = null;

    public Region(){}
    public Region(final double[] pLow, final double[] pHigh)
    {
        if (pLow.length != pHigh.length) throw new IllegalArgumentException("Region: arguments have different number of dimensions.");

        this.pLow = new double[pLow.length];
        System.arraycopy(pLow, 0, this.pLow, 0, pLow.length);

        this.pHigh = new double[pHigh.length];
        System.arraycopy(pHigh, 0, this.pHigh, 0, pHigh.length);
    }
    public Region(final Point low, final Point high)
    {
        if (low.pCoords.length != high.pCoords.length) throw new IllegalArgumentException("Region: arguments have different number of dimensions.");

        this.pLow = new double[low.pCoords.length];
        System.arraycopy(low.pCoords, 0, this.pLow, 0, low.pCoords.length);
        this.pHigh = new double[high.pCoords.length];
        System.arraycopy(high.pCoords, 0, this.pHigh, 0, high.pCoords.length);
    }
    public Region(final Region r)
    {
        this.pLow = new double[r.pLow.length];
        System.arraycopy(r.pLow, 0, this.pLow, 0, r.pLow.length);
        this.pHigh = new double[r.pHigh.length];
        System.arraycopy(r.pHigh, 0, this.pHigh, 0, r.pHigh.length);
    }
    public boolean equals(Object o)
    {
        if (o instanceof Region)
        {
            Region r = (Region) o;

            if (r.pLow.length != this.pLow.length) return false;

            for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
            {
                if (this.pLow[cIndex] < r.pLow[cIndex] - SpatialIndex.EPSILON || this.pLow[cIndex] > r.pLow[cIndex] + SpatialIndex.EPSILON ||
                        this.pHigh[cIndex] < r.pHigh[cIndex] - SpatialIndex.EPSILON || this.pHigh[cIndex] > r.pHigh[cIndex] + SpatialIndex.EPSILON)
                    return false;
            }
            return true;
        }
        return false;
    }
    public Object clone()
    {
        return new Region(this.pLow, this.pHigh);
    }
    public boolean intersects(final IShape s)
    {
        if (s instanceof Region) return intersects((Region) s);

        if (s instanceof Point) return contains((Point) s);

        throw new IllegalStateException("intersects: Not implemented yet!");
    }
    public boolean contains(final IShape s)
    {
        if (s instanceof Region) return contains((Region) s);

        if (s instanceof Point) return contains((Point) s);

        throw new IllegalStateException("contains: Not implemented yet!");
    }
    public boolean touches(final IShape s)
    {
        if (s instanceof Region) return touches((Region) s);

        if (s instanceof Point) return touches((Point) s);

        throw new IllegalStateException("touches: Not implemented yet!");
    }
    public double[] getCenter()
    {
        double[] pCoords = new double[this.pLow.length];

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            pCoords[cIndex] = (this.pLow[cIndex] + this.pHigh[cIndex]) / 2.0;
        }
        return pCoords;
    }
    public long getDimension()
    {
        return this.pLow.length;
    }
    public Region getMBR()
    {
        return new Region(this.pLow, this.pHigh);
    }
    public double getArea()
    {
        double area = 1.0;

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            area *= this.pHigh[cIndex] - this.pLow[cIndex];
        }
        return area;
    }
    public double getMinimumDistance(final IShape s)
    {
        if (s instanceof Region) return getMinimumDistance((Region) s);

        if (s instanceof Point) return getMinimumDistance((Point) s);

        throw new IllegalStateException("getMinimumDistance: Not implemented yet!");
    }
    public boolean intersects(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("intersects: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < pLow.length; cIndex++)
        {
            if (pLow[cIndex] > r.pHigh[cIndex] || pHigh[cIndex] < r.pLow[cIndex]) return false;
        }
        return true;
    }
    public boolean contains(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("contains: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if (this.pLow[cIndex] > r.pLow[cIndex] || this.pHigh[cIndex] < r.pHigh[cIndex]) return false;
        }
        return true;
    }
    public boolean touches(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("touches: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if ((this.pLow[cIndex] > r.pLow[cIndex] - SpatialIndex.EPSILON && this.pLow[cIndex] < r.pLow[cIndex] + SpatialIndex.EPSILON) ||
                    (this.pHigh[cIndex] > r.pHigh[cIndex] - SpatialIndex.EPSILON && this.pHigh[cIndex] < r.pHigh[cIndex] + SpatialIndex.EPSILON))
                return true;
        }
        return false;
    }
    public double getMinimumDistance(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("getMinimumDistance: Shape has the wrong number of dimensions.");

        double ret = 0.0;

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            double x = 0.0;

            if (r.pHigh[cIndex] < this.pLow[cIndex])
            {
                x = Math.abs(r.pHigh[cIndex] - this.pLow[cIndex]);
            }
            else if (this.pHigh[cIndex] < r.pLow[cIndex])
            {
                x = Math.abs(r.pLow[cIndex] - this.pHigh[cIndex]);
            }
            ret += x * x;
        }
        return Math.sqrt(ret);
    }
    public boolean contains(final Point p)
    {
        if (this.pLow.length != p.pCoords.length) throw new IllegalArgumentException("contains: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if (this.pLow[cIndex] > p.pCoords[cIndex] || this.pHigh[cIndex] < p.pCoords[cIndex]) return false;
        }
        return true;
    }
    public boolean touches(final Point p)
    {
        if (this.pLow.length != p.pCoords.length) throw new IllegalArgumentException("touches: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if ((this.pLow[cIndex] > p.pCoords[cIndex] - SpatialIndex.EPSILON && this.pLow[cIndex] < p.pCoords[cIndex] + SpatialIndex.EPSILON) ||
                    (this.pHigh[cIndex] > p.pCoords[cIndex] - SpatialIndex.EPSILON && this.pHigh[cIndex] < p.pCoords[cIndex] + SpatialIndex.EPSILON))
                return true;
        }
        return false;
    }
    public double getMinimumDistance(final Point p)
    {
        if (this.pLow.length != p.pCoords.length) throw new IllegalArgumentException("getMinimumDistance: Shape has the wrong number of dimensions.");

        double ret = 0.0;

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if (p.pCoords[cIndex] < this.pLow[cIndex])
            {
                ret += Math.pow(this.pLow[cIndex] - p.pCoords[cIndex], 2.0);
            }
            else if (p.pCoords[cIndex] > this.pHigh[cIndex])
            {
                ret += Math.pow(p.pCoords[cIndex] - this.pHigh[cIndex], 2.0);
            }
        }
        return Math.sqrt(ret);
    }
    public double getIntersectingArea(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("getIntersectingArea: Shape has the wrong number of dimensions.");

        int cIndex;

        // check for intersection.
        // marioh: avoid function call since this is called billions of times.
        for (cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            if (this.pLow[cIndex] > r.pHigh[cIndex] || this.pHigh[cIndex] < r.pLow[cIndex]) return 0.0;
        }

        double ret = 1.0;
        double f1, f2;

        for (cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            f1 = Math.max(this.pLow[cIndex], r.pLow[cIndex]);
            f2 = Math.min(this.pHigh[cIndex], r.pHigh[cIndex]);
            ret *= f2 - f1;
        }
        return ret;
    }
    public Region combinedRegion(final Region r)
    {
        if (this.pLow.length != r.pLow.length) throw new IllegalArgumentException("combinedRegion: Shape has the wrong number of dimensions.");

        double[] mn = new double[this.pLow.length];
        double[] mx = new double[this.pLow.length];

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            mn[cIndex] = Math.min(this.pLow[cIndex], r.pLow[cIndex]);
            mx[cIndex] = Math.max(this.pHigh[cIndex], r.pHigh[cIndex]);
        }
        return new Region(mn, mx);
    }
    public static Region combinedRegion(Region[] pRegions)
    {
        double[] mn = new double[pRegions[0].pLow.length];
        double[] mx = new double[pRegions[0].pLow.length];

        for (int cDim = 0; cDim < pRegions[0].pLow.length; cDim++)
        {
            mn[cDim] = Double.POSITIVE_INFINITY;
            mx[cDim] = Double.NEGATIVE_INFINITY;

            for (int cIndex = 0; cIndex < pRegions.length; cIndex++)
            {
                mn[cDim] = Math.min(mn[cDim], pRegions[cIndex].pLow[cDim]);
                mx[cDim] = Math.max(mx[cDim], pRegions[cIndex].pHigh[cDim]);
            }
        }
        return new Region(mn, mx);
    }
    public static void combinedRegion(Region pToModify, final Region pConst)
    {
        if (pToModify.pLow.length != pConst.pLow.length) throw new IllegalArgumentException("combineRegion: Shape has the wrong number of dimensions.");

        for (int cIndex = 0; cIndex < pToModify.pLow.length; cIndex++)
        {
            pToModify.pLow[cIndex] = Math.min(pToModify.pLow[cIndex], pConst.pLow[cIndex]);
            pToModify.pHigh[cIndex] = Math.max(pToModify.pHigh[cIndex], pConst.pHigh[cIndex]);
        }
    }
    public double getMargin()
    {
        double mul = Math.pow(2.0, ((double) this.pLow.length) - 1.0);
        double margin = 0.0;

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++)
        {
            margin += (this.pHigh[cIndex] - this.pLow[cIndex]) * mul;
        }

        return margin;
    }
    public double getLow(int index) throws IndexOutOfBoundsException
    {
        if (index >= this.pLow.length) throw new IndexOutOfBoundsException("" + index);
        return this.pLow[index];
    }

    public double getHigh(int index) throws IndexOutOfBoundsException
    {
        if (index >= this.pLow.length) throw new IndexOutOfBoundsException("" + index);
        return this.pHigh[index];
    }

    public String toString()
    {
        String s = "";

        for (int cIndex = 0; cIndex < this.pLow.length; cIndex++) s += this.pLow[cIndex] + " ";

        s += ": ";

        for (int cIndex = 0; cIndex < this.pHigh.length; cIndex++) s += this.pHigh[cIndex] + " ";

        return s;
    }

    public HashMap<byte[],Integer> queryKeyword(Region r){
        HashMap map=new HashMap();
        return map;
    }

}
