package rTree;

import spatialIndex.Region;
import spatialIndex.SpatialIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

public class Index extends Node{
    public Index(RTree pTree, int id, int level)
    {
        super(pTree, id, level, pTree.indexCapacity);
    }

    protected Node chooseSubtree(Region mbr, int level, Stack pathBuffer)
    {
        if (this.level == level) return this;

        pathBuffer.push(new Integer(identifier));

        int child = 0;

        switch (pTree.treeVariant)
        {
            case SpatialIndex.RtreeVariantLinear:
            case SpatialIndex.RtreeVariantQuadratic:
                child = findLeastEnlargement(mbr);
                break;
            case SpatialIndex.RtreeVariantRstar:
                if (this.level == 1)
                {
                    // if this node points to leaves...
                    child = findLeastOverlap(mbr);
                }
                else
                {
                    child = findLeastEnlargement(mbr);
                }
                break;
            default:
                throw new IllegalStateException("Unknown RTree variant.");
        }

        Node n = pTree.readNode(pIdentifier[child]);
        Node ret = n.chooseSubtree(mbr, level, pathBuffer);

        return ret;
    }

    protected Leaf findLeaf(Region mbr, int id, Stack pathBuffer)
    {
        pathBuffer.push(new Integer(identifier));

        for (int cChild = 0; cChild < children; cChild++)
        {
            if (pMBR[cChild].contains(mbr))
            {
                Node n = pTree.readNode(pIdentifier[cChild]);
                Leaf l = n.findLeaf(mbr, id, pathBuffer);
                if (l != null) return l;
            }
        }

        pathBuffer.pop();

        return null;
    }

    protected Node[] split(byte[] pData, Region mbr, int id)
    {
        pTree.stats.splits++;

        ArrayList g1 = new ArrayList(), g2 = new ArrayList();

        switch (pTree.treeVariant)
        {
            case SpatialIndex.RtreeVariantLinear:
            case SpatialIndex.RtreeVariantQuadratic:
                rtreeSplit(pData, mbr, id, g1, g2);
                break;
            case SpatialIndex.RtreeVariantRstar:
                rstarSplit(pData, mbr, id, g1, g2);
                break;
            default:
                throw new IllegalStateException("Unknown RTree variant.");
        }

        Node left = new Index(pTree, identifier, level);
        Node right = new Index(pTree, -1, level);

        int cIndex;

        for (cIndex = 0; cIndex < g1.size(); cIndex++)
        {
            int i = ((Integer) g1.get(cIndex)).intValue();
            left.insertEntry(null, pMBR[i], pIdentifier[i]);
        }

        for (cIndex = 0; cIndex < g2.size(); cIndex++)
        {
            int i = ((Integer) g2.get(cIndex)).intValue();
            right.insertEntry(null, pMBR[i], pIdentifier[i]);
        }

        Node[] ret = new Node[2];
        ret[0] = left;
        ret[1] = right;
        return ret;
    }

    protected int findLeastEnlargement(Region r)
    {
        double area = Double.POSITIVE_INFINITY;
        int best = -1;

        for (int cChild = 0; cChild < children; cChild++)
        {
            Region t = pMBR[cChild].combinedRegion(r);

            double a = pMBR[cChild].getArea();
            double enl = t.getArea() - a;

            if (enl < area)
            {
                area = enl;
                best = cChild;
            }
            else if (enl == area)
            {
                if (a < pMBR[best].getArea()) best = cChild;
            }
        }

        return best;
    }

    protected int findLeastOverlap(Region r)
    {
        OverlapEntry[] entries = new OverlapEntry[children];

        double leastOverlap = Double.POSITIVE_INFINITY;
        double me = Double.POSITIVE_INFINITY;
        int best = -1;

        // find combined region and enlargement of every entry and store it.
        for (int cChild = 0; cChild < children; cChild++)
        {
            OverlapEntry e = new OverlapEntry();

            e.id = cChild;
            e.original = pMBR[cChild];
            e.combined = pMBR[cChild].combinedRegion(r);
            e.oa = e.original.getArea();
            e.ca = e.combined.getArea();
            e.enlargement = e.ca - e.oa;
            entries[cChild] = e;

            if (e.enlargement < me)
            {
                me = e.enlargement;
                best = cChild;
            }
            else if (e.enlargement == me && e.oa < entries[best].oa)
            {
                best = cChild;
            }
        }

        if (me < SpatialIndex.EPSILON || me > SpatialIndex.EPSILON)
        {
            int cIterations;

            if (children > pTree.nearMinimumOverlapFactor)
            {
                // sort entries in increasing order of enlargement.
                Arrays.sort(entries, new OverlapEntryComparator());
                cIterations = pTree.nearMinimumOverlapFactor;
            }
            else
            {
                cIterations = children;
            }

            // calculate overlap of most important original entries (near minimum overlap cost).
            for (int cIndex = 0; cIndex < cIterations; cIndex++)
            {
                double dif = 0.0;
                OverlapEntry e = entries[cIndex];

                for (int cChild = 0; cChild < children; cChild++)
                {
                    if (e.id != cChild)
                    {
                        double f = e.combined.getIntersectingArea(pMBR[cChild]);
                        if (f != 0.0) dif +=  f - e.original.getIntersectingArea(pMBR[cChild]);
                    }
                } // for (cChild)

                if (dif < leastOverlap)
                {
                    leastOverlap = dif;
                    best = cIndex;
                }
                else if (dif == leastOverlap)
                {
                    if (e.enlargement == entries[best].enlargement)
                    {
                        // keep the one with least area.
                        if (e.original.getArea() < entries[best].original.getArea()) best = cIndex;
                    }
                    else
                    {
                        // keep the one with least enlargement.
                        if (e.enlargement < entries[best].enlargement) best = cIndex;
                    }
                }
            } // for (cIndex)
        }

        return entries[best].id;
    }

    protected void adjustTree(Node n, Stack pathBuffer)
    {
        pTree.stats.adjustments++;

        // find entry pointing to old node;
        int child;
        for (child = 0; child < children; child++)
        {
            if (pIdentifier[child] == n.identifier) break;
        }

        // MBR needs recalculation if either:
        //   1. the NEW child MBR is not contained.
        //   2. the OLD child MBR is touching.
        boolean b = nodeMBR.contains(n.nodeMBR);
        boolean recalc = (! b) ? true : nodeMBR.touches(pMBR[child]);

        pMBR[child] = (Region) n.nodeMBR.clone();

        if (recalc)
        {
            for (int cDim = 0; cDim < pTree.dimension; cDim++)
            {
                nodeMBR.pLow[cDim] = Double.POSITIVE_INFINITY;
                nodeMBR.pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < children; cChild++)
                {
                    nodeMBR.pLow[cDim] = Math.min(nodeMBR.pLow[cDim], pMBR[cChild].pLow[cDim]);
                    nodeMBR.pHigh[cDim] = Math.max(nodeMBR.pHigh[cDim], pMBR[cChild].pHigh[cDim]);
                }
            }
        }

        pTree.writeNode(this);

        if (recalc && ! pathBuffer.empty())
        {
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) pTree.readNode(cParent);
            p.adjustTree(this, pathBuffer);
        }
    }

    protected void adjustTree(Node n1, Node n2, Stack pathBuffer, boolean[] overflowTable)
    {
        pTree.stats.adjustments++;

        // find entry pointing to old node;
        int child;
        for (child = 0; child < children; child++)
        {
            if (pIdentifier[child] == n1.identifier) break;
        }

        // MBR needs recalculation if either:
        //   1. the NEW child MBR is not contained.
        //   2. the OLD child MBR is touching.
        boolean b = nodeMBR.contains(n1.nodeMBR);
        boolean recalc = (! b) ? true : nodeMBR.touches(pMBR[child]);

        pMBR[child] = (Region) n1.nodeMBR.clone();

        if (recalc)
        {
            for (int cDim = 0; cDim < pTree.dimension; cDim++)
            {
                nodeMBR.pLow[cDim] = Double.POSITIVE_INFINITY;
                nodeMBR.pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < children; cChild++)
                {
                    nodeMBR.pLow[cDim] = Math.min(nodeMBR.pLow[cDim], pMBR[cChild].pLow[cDim]);
                    nodeMBR.pHigh[cDim] = Math.max(nodeMBR.pHigh[cDim], pMBR[cChild].pHigh[cDim]);
                }
            }
        }

        // No write necessary here. insertData will write the node if needed.
        //m_pTree.writeNode(this);

        boolean adjusted = insertData(null, (Region) n2.nodeMBR.clone(), n2.identifier, pathBuffer, overflowTable);

        // if n2 is contained in the node and there was no split or reinsert,
        // we need to adjust only if recalculation took place.
        // In all other cases insertData above took care of adjustment.
        if (! adjusted && recalc && ! pathBuffer.empty())
        {
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) pTree.readNode(cParent);
            p.adjustTree(this, pathBuffer);
        }
    }

    class OverlapEntry
    {
        int id;
        double enlargement;
        Region original;
        Region combined;
        double oa;
        double ca;
    }

    class OverlapEntryComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            OverlapEntry e1 = (OverlapEntry) o1;
            OverlapEntry e2 = (OverlapEntry) o2;

            if (e1.enlargement < e2.enlargement) return -1;
            if (e1.enlargement > e2.enlargement) return 1;
            return 0;
        }
    }

}
