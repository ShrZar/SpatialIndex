package rTree;

import spatialIndex.*;

import java.io.*;
import java.util.*;

public abstract class Node implements INode {
    protected RTree pTree = null;//节点所在的树
    protected int level = -1;//节点在树中的层次，叶节点为0层
    protected int identifier = -1;//每个节点的编号
    protected int children = 0;//该节点的子节点数量
    protected int capacity = -1;//节点容量
    protected Region nodeMBR = null;//该节点中数据的最小生成矩形
    protected byte[][] pData = null;//节点中的数据
    protected Region[] pMBR = null;//该节点中所有的矩形
    protected int[] pIdentifier = null;//该节点中各数据对应的编号
    protected int[] pDataLength = null;//节点中各数据的长度
    int totalDataLength = 0;//总数据长度

    protected abstract Node chooseSubtree(Region mbr, int level, Stack pathBuffer);
    protected abstract Leaf findLeaf(Region mbr, int id, Stack pathBuffer);
    protected abstract Node[] split(byte[] pData, Region mbr, int id);

    public int getIdentifier()
    {
        return identifier;
    }

    public IShape getShape()
    {
        return (IShape) nodeMBR.clone();
    }

    public int getChildrenCount()
    {
        return children;
    }

    public Region[] getpMBR(){return pMBR;}

    public int getChildIdentifier(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= children) throw new IndexOutOfBoundsException("" + index);

        return pIdentifier[index];
    }

    public IShape getChildShape(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= children) throw new IndexOutOfBoundsException("" + index);

        return new Region(pMBR[index]);
    }

    public int getLevel()
    {
        return level;
    }

    public boolean isLeaf()
    {
        return (level == 0);
    }

    public boolean isIndex()
    {
        return (level != 0);
    }


    protected Node(RTree pTree, int id, int level, int capacity)
    {
        this.pTree = pTree;
        this.level = level;
        this.identifier = id;
        this.capacity = capacity;
        this.nodeMBR = (Region) pTree.infiniteRegion.clone();

        this.pDataLength = new int[this.capacity + 1];
        this.pData = new byte[this.capacity + 1][];
        this.pMBR = new Region[this.capacity + 1];
        this.pIdentifier = new int[this.capacity + 1];
    }

    /**
     * 向节点插入新的实体，如果插入成功再生成新的MBR
     * @param pData
     * @param mbr
     * @param id
     * @throws IllegalStateException
     */
    protected void insertEntry(byte[] pData, Region mbr, int id) throws IllegalStateException
    {
        if (this.children >= this.capacity) throw new IllegalStateException("children >= nodeCapacity");

        this.pDataLength[this.children] = (pData != null) ? pData.length : 0;
        this.pData[this.children] = pData;
        this.pMBR[this.children] = mbr;
        this.pIdentifier[this.children] = id;

        this.totalDataLength += this.pDataLength[this.children];
        this.children++;

        Region.combinedRegion(this.nodeMBR, mbr);
    }

    /**
     * 从节点中删除指定下标的实体，把最后的数据提到该位置，同时调整MBR
     * @param index
     * @throws IndexOutOfBoundsException
     */
    protected void deleteEntry(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= this.children) throw new IndexOutOfBoundsException("" + index);

        boolean touches = this.nodeMBR.touches(this.pMBR[index]);

        this.totalDataLength -= this.pDataLength[index];
        this.pData[index] = null;

        if (this.children > 1 && index != this.children - 1)
        {
            this.pDataLength[index] = this.pDataLength[this.children - 1];
            this.pData[index] = this.pData[this.children - 1];
            this.pData[this.children - 1] = null;
            this.pMBR[index] = this.pMBR[this.children - 1];
            this.pMBR[this.children - 1] = null;
            this.pIdentifier[index] = this.pIdentifier[this.children - 1];
        }

        this.children--;

        if (this.children == 0)
        {
            this.nodeMBR = (Region) this.pTree.infiniteRegion.clone();
        }
        else if (touches)
        {
            for (int cDim = 0; cDim < this.pTree.dimension; cDim++)
            {
                this.nodeMBR.pLow[cDim] = Double.POSITIVE_INFINITY;
                this.nodeMBR.pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < this.children; cChild++)
                {
                    this.nodeMBR.pLow[cDim] = Math.min(this.nodeMBR.pLow[cDim], this.pMBR[cChild].pLow[cDim]);
                    this.nodeMBR.pHigh[cDim] = Math.max(this.nodeMBR.pHigh[cDim], this.pMBR[cChild].pHigh[cDim]);
                }
            }
        }
    }


    protected boolean insertData(byte[] pData, Region mbr, int id, Stack pathBuffer, boolean[] overflowTable)
    {
        if (children < capacity)//如果子节点数量没有超过节点容量，插入数据，写入节点
        {
            boolean adjusted = false;
            boolean b = nodeMBR.contains(mbr);

            insertEntry(pData, mbr, id);
            pTree.writeNode(this);

            if (! b && ! pathBuffer.empty())//如果数据的MBR不在节点的MBR内且缓存不为空，则需找到其父索引节点并调整树
            {
                int cParent = ((Integer) pathBuffer.pop()).intValue();
                Index p = (Index) pTree.readNode(cParent);
                p.adjustTree(this, pathBuffer);
                adjusted = true;
            }

            return adjusted;
        }
        else if (pTree.treeVariant == SpatialIndex.RtreeVariantRstar && ! pathBuffer.empty() && overflowTable[level] == false)
        //如果该树是R*树且缓存不为空且该层的溢出标志位为false
        {
            overflowTable[level] = true;

            ArrayList vReinsert = new ArrayList(), vKeep = new ArrayList();
            reinsertData(pData, mbr, id, vReinsert, vKeep);

            int lReinsert = vReinsert.size();
            int lKeep = vKeep.size();

            byte[][] reinsertdata = new byte[lReinsert][];
            Region[] reinsertmbr = new Region[lReinsert];
            int[] reinsertid = new int[lReinsert];
            int[] reinsertlen = new int[lReinsert];
            byte[][] keepdata = new byte[capacity + 1][];
            Region[] keepmbr = new Region[capacity + 1];
            int[] keepid = new int[capacity + 1];
            int[] keeplen = new int[capacity + 1];

            int cIndex;

            for (cIndex = 0; cIndex < lReinsert; cIndex++)
            {
                int i = ((Integer) vReinsert.get(cIndex)).intValue();
                reinsertlen[cIndex] = pDataLength[i];
                reinsertdata[cIndex] = this.pData[i];
                reinsertmbr[cIndex] = pMBR[i];
                reinsertid[cIndex] = pIdentifier[i];
            }

            for (cIndex = 0; cIndex < lKeep; cIndex++)
            {
                int i = ((Integer) vKeep.get(cIndex)).intValue();
                keeplen[cIndex] = pDataLength[i];
                keepdata[cIndex] = this.pData[i];
                keepmbr[cIndex] = pMBR[i];
                keepid[cIndex] = pIdentifier[i];
            }

            pDataLength = keeplen;
            this.pData = keepdata;
            this.pMBR = keepmbr;
            pIdentifier = keepid;
            children = lKeep;
            totalDataLength = 0;
            for (int cChild = 0; cChild < children; cChild++) totalDataLength += pDataLength[cChild];

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

            pTree.writeNode(this);

            // Divertion from R*-Tree algorithm here. First adjust
            // the path to the root, then start reinserts, to avoid complicated handling
            // of changes to the same node from multiple insertions.
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) pTree.readNode(cParent);
            p.adjustTree(this, pathBuffer);

            for (cIndex = 0; cIndex < lReinsert; cIndex++)
            {
                pTree.insertData_impl(reinsertdata[cIndex],
                        reinsertmbr[cIndex],
                        reinsertid[cIndex],
                        level, overflowTable);
            }

            return true;
        }
        else
        {
            Node[] nodes = split(pData, mbr, id);
            Node n = nodes[0];
            Node nn = nodes[1];

            if (pathBuffer.empty())
            {
                n.identifier = -1;
                nn.identifier = -1;
                pTree.writeNode(n);
                pTree.writeNode(nn);

                Index r = new Index(pTree, pTree.rootID, level + 1);

                r.insertEntry(null, (Region) n.nodeMBR.clone(), n.identifier);
                r.insertEntry(null, (Region) nn.nodeMBR.clone(), nn.identifier);

                pTree.writeNode(r);

                pTree.stats.nodesInLevel.set(level, new Integer(2));
                pTree.stats.nodesInLevel.add(new Integer(1));
                pTree.stats.treeHeight = level + 2;
            }
            else
            {
                n.identifier = identifier;
                nn.identifier = -1;

                pTree.writeNode(n);
                pTree.writeNode(nn);

                int cParent = ((Integer) pathBuffer.pop()).intValue();
                Index p = (Index) pTree.readNode(cParent);
                p.adjustTree(n, nn, pathBuffer, overflowTable);
            }

            return true;
        }
    }

    /**
     *
     * @param pData
     * @param mbr
     * @param id
     * @param reinsert
     * @param keep
     */
    protected void reinsertData(byte[] pData, Region mbr, int id, ArrayList reinsert, ArrayList keep)
    {
        ReinsertEntry[] v = new ReinsertEntry[capacity + 1];

        pDataLength[children] = (pData != null) ? pData.length : 0;
        this.pData[children] = pData;
        pMBR[children] = mbr;
        pIdentifier[children] = id;

        double[] nc = nodeMBR.getCenter();

        for (int cChild = 0; cChild < capacity + 1; cChild++)
        {
            ReinsertEntry e = new ReinsertEntry(cChild, 0.0f);

            double[] c = pMBR[cChild].getCenter();

            // calculate relative distance of every entry from the node MBR (ignore square root.)
            for (int cDim = 0; cDim < pTree.dimension; cDim++)
            {
                double d = nc[cDim] - c[cDim];
                e.dist += d * d;
            }

            v[cChild] = e;
        }

        // sort by increasing order of distances.
        Arrays.sort(v, new ReinsertEntryComparator());

        int cReinsert = (int) Math.floor((capacity + 1) * pTree.reinsertFactor);
        int cCount;

        for (cCount = 0; cCount < cReinsert; cCount++)
        {
            reinsert.add(new Integer(v[cCount].id));
        }

        for (cCount = cReinsert; cCount < capacity + 1; cCount++)
        {
            keep.add(new Integer(v[cCount].id));
        }
    }

    protected void rtreeSplit(byte[] pData, Region mbr, int id, ArrayList group1, ArrayList group2)
    {
        int cChild;
        int minimumLoad = (int) Math.floor(capacity * pTree.fillFactor);

        // use this mask array for marking visited entries.
        boolean[] mask = new boolean[capacity + 1];
        for (cChild = 0; cChild < capacity + 1; cChild++) mask[cChild] = false;

        // insert new data in the node for easier manipulation. Data arrays are always
        // by one larger than node capacity.
        pDataLength[capacity] = (pData != null) ? pData.length : 0;
        this.pData[capacity] = pData;
        pMBR[capacity] = mbr;
        pIdentifier[capacity] = id;

        // initialize each group with the seed entries.
        int[] seeds = pickSeeds();

        group1.add(new Integer(seeds[0]));
        group2.add(new Integer(seeds[1]));

        mask[seeds[0]] = true;
        mask[seeds[1]] = true;

        // find MBR of each group.
        Region mbr1 = (Region) pMBR[seeds[0]].clone();
        Region mbr2 = (Region) pMBR[seeds[1]].clone();

        // count how many entries are left unchecked (exclude the seeds here.)
        int cRemaining = capacity + 1 - 2;

        while (cRemaining > 0)
        {
            if (minimumLoad - group1.size() == cRemaining)
            {
                // all remaining entries must be assigned to group1 to comply with minimun load requirement.
                for (cChild = 0; cChild < capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        group1.add(new Integer(cChild));
                        mask[cChild] = true;
                        cRemaining--;
                    }
                }
            }
            else if (minimumLoad - group2.size() == cRemaining)
            {
                // all remaining entries must be assigned to group2 to comply with minimun load requirement.
                for (cChild = 0; cChild < capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        group2.add(new Integer(cChild));
                        mask[cChild] = true;
                        cRemaining--;
                    }
                }
            }
            else
            {
                // For all remaining entries compute the difference of the cost of grouping an
                // entry in either group. When done, choose the entry that yielded the maximum
                // difference. In case of linear split, select any entry (e.g. the first one.)
                int sel = -1;
                double md1 = 0.0f, md2 = 0.0f;
                double m = Double.NEGATIVE_INFINITY;
                double d1, d2, d;
                double a1 = mbr1.getArea();
                double a2 = mbr2.getArea();

                for (cChild = 0; cChild < capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        Region a = mbr1.combinedRegion(pMBR[cChild]);
                        d1 = a.getArea() - a1;
                        Region b = mbr2.combinedRegion(pMBR[cChild]);
                        d2 = b.getArea() - a2;
                        d = Math.abs(d1 - d2);

                        if (d > m)
                        {
                            m = d;
                            md1 = d1; md2 = d2;
                            sel = cChild;
                            if (pTree.treeVariant== SpatialIndex.RtreeVariantLinear || pTree.treeVariant == SpatialIndex.RtreeVariantRstar) break;
                        }
                    }
                }

                // determine the group where we should add the new entry.
                int group = -1;

                if (md1 < md2)
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (md2 < md1)
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                else if (a1 < a2)
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (a2 < a1)
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                else if (group1.size() < group2.size())
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (group2.size() < group1.size())
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                else
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                mask[sel] = true;
                cRemaining--;
                if (group == 1)
                {
                    Region.combinedRegion(mbr1, pMBR[sel]);
                }
                else
                {
                    Region.combinedRegion(mbr2, pMBR[sel]);
                }
            }
        }
    }

    protected void rstarSplit(byte[] pData, Region mbr, int id, ArrayList group1, ArrayList group2)
    {
        RstarSplitEntry[] dataLow = new RstarSplitEntry[capacity + 1];;
        RstarSplitEntry[] dataHigh = new RstarSplitEntry[capacity + 1];;

        pDataLength[children] = (pData != null) ? pData.length : 0;
        this.pData[capacity] = pData;
        pMBR[capacity] = mbr;
        pIdentifier[capacity] = id;

        int nodeSPF = (int) (Math.floor((capacity + 1) * pTree.splitDistributionFactor));
        int splitDistribution = (capacity + 1) - (2 * nodeSPF) + 2;

        int cChild, cDim, cIndex;

        for (cChild = 0; cChild < capacity + 1; cChild++)
        {
            RstarSplitEntry e = new RstarSplitEntry(pMBR[cChild], cChild, 0);

            dataLow[cChild] = e;
            dataHigh[cChild] = e;
        }

        double minimumMargin = Double.POSITIVE_INFINITY;
        int splitAxis = -1;
        int sortOrder = -1;

        // chooseSplitAxis.
        for (cDim = 0; cDim < pTree.dimension; cDim++)
        {
            Arrays.sort(dataLow, new RstarSplitEntryComparatorLow());
            Arrays.sort(dataHigh, new RstarSplitEntryComparatorHigh());

            // calculate sum of margins and overlap for all distributions.
            double marginl = 0.0;
            double marginh = 0.0;

            for (cChild = 1; cChild <= splitDistribution; cChild++)
            {
                int l = nodeSPF - 1 + cChild;

                Region[] tl1 = new Region[l];
                Region[] th1 = new Region[l];

                for (cIndex = 0; cIndex < l; cIndex++)
                {
                    tl1[cIndex] = dataLow[cIndex].pRegion;
                    th1[cIndex] = dataHigh[cIndex].pRegion;
                }

                Region bbl1 = Region.combinedRegion(tl1);
                Region bbh1 = Region.combinedRegion(th1);

                Region[] tl2 = new Region[capacity + 1 - l];
                Region[] th2 = new Region[capacity + 1 - l];

                int tmpIndex = 0;
                for (cIndex = l; cIndex < capacity + 1; cIndex++)
                {
                    tl2[tmpIndex] = dataLow[cIndex].pRegion;
                    th2[tmpIndex] = dataHigh[cIndex].pRegion;
                    tmpIndex++;
                }

                Region bbl2 = Region.combinedRegion(tl2);
                Region bbh2 = Region.combinedRegion(th2);

                marginl += bbl1.getMargin() + bbl2.getMargin();
                marginh += bbh1.getMargin() + bbh2.getMargin();
            } // for (cChild)

            double margin = Math.min(marginl, marginh);

            // keep minimum margin as split axis.
            if (margin < minimumMargin)
            {
                minimumMargin = margin;
                splitAxis = cDim;
                sortOrder = (marginl < marginh) ? 0 : 1;
            }

            // increase the dimension according to which the data entries should be sorted.
            for (cChild = 0; cChild < capacity + 1; cChild++)
            {
                dataLow[cChild].sortDim = cDim + 1;
            }
        } // for (cDim)

        for (cChild = 0; cChild < capacity + 1; cChild++)
        {
            dataLow[cChild].sortDim = splitAxis;
        }

        if (sortOrder == 0)
            Arrays.sort(dataLow, new RstarSplitEntryComparatorLow());
        else
            Arrays.sort(dataLow, new RstarSplitEntryComparatorHigh());

        double ma = Double.POSITIVE_INFINITY;
        double mo = Double.POSITIVE_INFINITY;
        int splitPoint = -1;

        for (cChild = 1; cChild <= splitDistribution; cChild++)
        {
            int l = nodeSPF - 1 + cChild;

            Region[] t1 = new Region[l];

            for (cIndex = 0; cIndex < l; cIndex++)
            {
                t1[cIndex] = dataLow[cIndex].pRegion;
            }

            Region bb1 = Region.combinedRegion(t1);

            Region[] t2 = new Region[capacity + 1 - l];

            int tmpIndex = 0;
            for (cIndex = l; cIndex < capacity + 1; cIndex++)
            {
                t2[tmpIndex] = dataLow[cIndex].pRegion;
                tmpIndex++;
            }

            Region bb2 = Region.combinedRegion(t2);

            double o = bb1.getIntersectingArea(bb2);

            if (o < mo)
            {
                splitPoint = cChild;
                mo = o;
                ma = bb1.getArea() + bb2.getArea();
            }
            else if (o == mo)
            {
                double a = bb1.getArea() + bb2.getArea();

                if (a < ma)
                {
                    splitPoint = cChild;
                    ma = a;
                }
            }
        } // for (cChild)

        int l1 = nodeSPF - 1 + splitPoint;

        for (cIndex = 0; cIndex < l1; cIndex++)
        {
            group1.add(new Integer(dataLow[cIndex].id));
        }

        for (cIndex = l1; cIndex <= capacity; cIndex++)
        {
            group2.add(new Integer(dataLow[cIndex].id));
        }
    }

    protected int[] pickSeeds()
    {
        double separation = Double.NEGATIVE_INFINITY;
        double inefficiency = Double.NEGATIVE_INFINITY;
        int cDim, cChild, cIndex, i1 = 0, i2 = 0;

        switch (pTree.treeVariant)
        {
            case SpatialIndex.RtreeVariantLinear:
            case SpatialIndex.RtreeVariantRstar:
                for (cDim = 0; cDim < pTree.dimension; cDim++)
                {
                    double leastLower = pMBR[0].pLow[cDim];
                    double greatestUpper = pMBR[0].pHigh[cDim];
                    int greatestLower = 0;
                    int leastUpper = 0;
                    double width;

                    for (cChild = 1; cChild < capacity + 1; cChild++)
                    {
                        if (pMBR[cChild].pLow[cDim] > pMBR[greatestLower].pLow[cDim]) greatestLower = cChild;
                        if (pMBR[cChild].pHigh[cDim] < pMBR[leastUpper].pHigh[cDim]) leastUpper = cChild;

                        leastLower = Math.min(pMBR[cChild].pLow[cDim], leastLower);
                        greatestUpper = Math.max(pMBR[cChild].pHigh[cDim], greatestUpper);
                    }

                    width = greatestUpper - leastLower;
                    if (width <= 0) width = 1;

                    double f = (pMBR[greatestLower].pLow[cDim] - pMBR[leastUpper].pHigh[cDim]) / width;

                    if (f > separation)
                    {
                        i1 = leastUpper;
                        i2 = greatestLower;
                        separation = f;
                    }
                }  // for (cDim)

                if (i1 == i2)
                {
                    i2 = (i2 != capacity) ? i2 + 1 : i2 - 1;
                }

                break;
            case SpatialIndex.RtreeVariantQuadratic:
                // for each pair of Regions (account for overflow Region too!)
                for (cChild = 0; cChild < capacity; cChild++)
                {
                    double a = pMBR[cChild].getArea();

                    for (cIndex = cChild + 1; cIndex < capacity + 1; cIndex++)
                    {
                        // get the combined MBR of those two entries.
                        Region r = pMBR[cChild].combinedRegion(pMBR[cIndex]);

                        // find the inefficiency of grouping these entries together.
                        double d = r.getArea() - a - pMBR[cIndex].getArea();

                        if (d > inefficiency)
                        {
                            inefficiency = d;
                            i1 = cChild;
                            i2 = cIndex;
                        }
                    }  // for (cIndex)
                } // for (cChild)

                break;
            default:
                throw new IllegalStateException("Unknown RTree variant.");
        }

        int[] ret = new int[2];
        ret[0] = i1;
        ret[1] = i2;
        return ret;
    }

    protected void condenseTree(Stack toReinsert, Stack pathBuffer)
    {
        int minimumLoad = (int) (Math.floor(capacity * pTree.fillFactor));

        if (pathBuffer.empty())
        {
            // eliminate root if it has only one child.
            if (level != 0 && children == 1)
            {
                Node n = pTree.readNode(pIdentifier[0]);
                pTree.deleteNode(n);
                n.identifier = pTree.rootID;
                pTree.writeNode(n);

                pTree.stats.nodesInLevel.remove(pTree.stats.nodesInLevel.size() - 1);
                pTree.stats.treeHeight -= 1;
                // HACK: pending deleteNode for deleted child will decrease nodesInLevel, later on.
                pTree.stats.nodesInLevel.set(pTree.stats.treeHeight - 1, new Integer(2));
            }
        }
        else
        {
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) pTree.readNode(cParent);

            // find the entry in the parent, that points to this node.
            int child;

            for (child = 0; child != p.children; child++)
            {
                if (p.pIdentifier[child] == identifier) break;
            }

            if (children < minimumLoad)
            {
                // used space less than the minimum
                // 1. eliminate node entry from the parent. deleteEntry will fix the parent's MBR.
                p.deleteEntry(child);
                // 2. add this node to the stack in order to reinsert its entries.
                toReinsert.push(this);
            }
            else
            {
                // adjust the entry in 'p' to contain the new bounding region of this node.
                p.pMBR[child] = (Region) nodeMBR.clone();

                // global recalculation necessary since the MBR can only shrink in size,
                // due to data removal.
                for (int cDim = 0; cDim < pTree.dimension; cDim++)
                {
                    p.nodeMBR.pLow[cDim] = Double.POSITIVE_INFINITY;
                    p.nodeMBR.pHigh[cDim] = Double.NEGATIVE_INFINITY;

                    for (int cChild = 0; cChild < p.children; cChild++)
                    {
                        p.nodeMBR.pLow[cDim] = Math.min(p.nodeMBR.pLow[cDim], p.pMBR[cChild].pLow[cDim]);
                        p.nodeMBR.pHigh[cDim] = Math.max(p.nodeMBR.pHigh[cDim], p.pMBR[cChild].pHigh[cDim]);
                    }
                }
            }

            // write parent node back to storage.
            pTree.writeNode(p);

            p.condenseTree(toReinsert, pathBuffer);
        }
    }

    /**
     * 从输入流中读入节点信息
     * @param data
     * @throws IOException
     */
    protected void load(byte[] data) throws IOException
    {
        nodeMBR = (Region) pTree.infiniteRegion.clone();

        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));

        // skip the node type information, it is not needed.
        ds.readInt();

        level = ds.readInt();
        children = ds.readInt();

        for (int cChild = 0; cChild < children; cChild++)
        {
            pMBR[cChild] = new Region();
            pMBR[cChild].pLow = new double[pTree.dimension];
            pMBR[cChild].pHigh = new double[pTree.dimension];

            for (int cDim = 0; cDim < pTree.dimension; cDim++)
            {
                pMBR[cChild].pLow[cDim] = ds.readDouble();
                pMBR[cChild].pHigh[cDim] = ds.readDouble();
            }

            pIdentifier[cChild] = ds.readInt();

            pDataLength[cChild] = ds.readInt();
            if (pDataLength[cChild] > 0)
            {
                totalDataLength += pDataLength[cChild];
                pData[cChild] = new byte[pDataLength[cChild]];
                ds.read(pData[cChild]);
            }
            else
            {
                pData[cChild] = null;
            }

            Region.combinedRegion(nodeMBR, pMBR[cChild]);
        }
    }

    /**
     * 将节点信息存入输出流
     * @return
     * @throws IOException
     */
    protected byte[] store() throws IOException
    {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);

        int type;
        if (level == 0) type = SpatialIndex.PersistentLeaf;
        else type = SpatialIndex.PersistentIndex;
        ds.writeInt(type);

        ds.writeInt(level);
        ds.writeInt(children);

        for (int cChild = 0; cChild < children; cChild++)
        {
            for (int cDim = 0; cDim < pTree.dimension; cDim++)
            {
                ds.writeDouble(pMBR[cChild].pLow[cDim]);
                ds.writeDouble(pMBR[cChild].pHigh[cDim]);
            }

            ds.writeInt(pIdentifier[cChild]);

            ds.writeInt(pDataLength[cChild]);
            if (pDataLength[cChild] > 0) ds.write(pData[cChild]);
        }

        ds.flush();
        return bs.toByteArray();
    }

    public String toString(){
//        int dataLength=0;
//        for (int i=0;i<children;i++)
//            dataLength+=pd
        byte[] data=new byte[totalDataLength];
        int destPos=0;
        for (int i=0;i<children;i++) {
            System.arraycopy(pData[i], 0, data, destPos, pDataLength[i]);
            destPos+=pDataLength[i];
        }

        String da=new String(data);
        String s="Level:"+level+"\n"
                +"Identifier:"+identifier+"\n"
                +"Children:"+children+"\n"
                +"Region:"+nodeMBR.toString()+"\n"
                +"pIdentifier:"+Arrays.toString(pIdentifier)+"\n"
                +"Data:"+da;
        return s;
    }

    public List countKeyword(HashMap<String,Integer> map,Region r){
        if (this.isLeaf()){
            for (int i=0;i<children;i++){
                Region ri=this.pMBR[i];
                if (r.contains(new Point(ri.pLow))) {
                    String key=new String(pData[i]).split(",")[0];
                    map.put(key,map.getOrDefault(key,0)+1);
                }
                if (r.contains(new Point(ri.pHigh))){
                    String key=new String(pData[i]).split(",")[1];
                    map.put(key,map.getOrDefault(key,0)+1);
                }
            }
//            String[] words=new String(pData[children-1]).split(",");
//            if (r.contains(new Point(nodeMBR.pLow)))
//                map.put(words[0],map.getOrDefault(words[0],0)+1);
//            if (r.contains(new Point(nodeMBR.pHigh)))
//                map.put(words[1],map.getOrDefault(words[1],0)+1);
//            return;
        }else if(this.isIndex()&&this.nodeMBR.intersects(r)){
            for (int i = 0; i < children; i++) {
                Node n = pTree.readNode(pIdentifier[i]);
                n.countKeyword(map, r);
            }
        }
        Map<String,Integer> mmap=new HashMap<>(map);
        List<Map.Entry<String,Integer>> list= new ArrayList<Map.Entry<String, Integer>>(mmap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return list;
    }


    class ReinsertEntry
    {
        int id;
        double dist;
        public ReinsertEntry(int id, double dist) { this.id = id; this.dist = dist; }
    } // ReinsertEntry

    class ReinsertEntryComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            if (((ReinsertEntry) o1).dist < ((ReinsertEntry) o2).dist) return -1;
            if (((ReinsertEntry) o1).dist > ((ReinsertEntry) o2).dist) return 1;
            return 0;
        }
    } // ReinsertEntryComparator

    class RstarSplitEntry
    {
        Region pRegion;
        int id;
        int sortDim;

        RstarSplitEntry(Region r, int id, int dimension) { pRegion = r; this.id = id; sortDim = dimension; }
    }

    class RstarSplitEntryComparatorLow implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            RstarSplitEntry e1 = (RstarSplitEntry) o1;
            RstarSplitEntry e2 = (RstarSplitEntry) o2;

            if (e1.pRegion.pLow[e1.sortDim] < e2.pRegion.pLow[e2.sortDim]) return -1;
            if (e1.pRegion.pLow[e1.sortDim] > e2.pRegion.pLow[e2.sortDim]) return 1;
            return 0;
        }
    }

    class RstarSplitEntryComparatorHigh implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            RstarSplitEntry e1 = (RstarSplitEntry) o1;
            RstarSplitEntry e2 = (RstarSplitEntry) o2;

            if (e1.pRegion.pHigh[e1.sortDim] < e2.pRegion.pHigh[e2.sortDim]) return -1;
            if (e1.pRegion.pHigh[e1.sortDim] > e2.pRegion.pHigh[e2.sortDim]) return 1;
            return 0;
        }
    }
}
