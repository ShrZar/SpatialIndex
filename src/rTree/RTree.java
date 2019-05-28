package rTree;

import spatialIndex.*;
import storageManager.IStorageManager;
import storageManager.InvalidPageException;
import storageManager.PropertySet;

import java.io.*;
import java.util.*;

public class RTree implements ISpatialIndex {
    RWLock rwLock;

    IStorageManager pStorageManager;

    public int rootID;
    int headerID;

    int treeVariant;

    double fillFactor;

    int indexCapacity;

    int leafCapacity;

    int nearMinimumOverlapFactor;
    // The R*-Tree 'p' constant, for calculating nearly minimum overlap cost.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    // for Points and Rectangles, Section 4.1]

    double splitDistributionFactor;
    // The R*-Tree 'm' constant, for calculating spliting distributions.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    // for Points and Rectangles, Section 4.2]

    double reinsertFactor;
    // The R*-Tree 'p' constant, for removing entries at reinserts.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    //  for Points and Rectangles, Section 4.3]

    int dimension;

    Region infiniteRegion;

    public Statistics stats;

    ArrayList writeNodeCommands = new ArrayList();
    ArrayList readNodeCommands = new ArrayList();
    ArrayList deleteNodeCommands = new ArrayList();

    public RTree(PropertySet ps, IStorageManager sm)
    {
        rwLock = new RWLock();

        pStorageManager = sm;
        rootID = IStorageManager.NewPage;
        headerID = IStorageManager.NewPage;
        treeVariant = SpatialIndex.RtreeVariantRstar;
//        treeVariant=SpatialIndex.RtreeVariantLinear;
        fillFactor = 1;//0.7f;
        indexCapacity = 5;//100;
        leafCapacity = 5;//100;
        nearMinimumOverlapFactor = 32;
        splitDistributionFactor = 0.4f;
        reinsertFactor = 0.3f;
        dimension = 2;

        infiniteRegion = new Region();
        stats = new Statistics();

        Object var = ps.getProperty("IndexIdentifier");
        if (var != null)
        {
            if (! (var instanceof Integer)) throw new IllegalArgumentException("Property IndexIdentifier must an Integer");
            headerID = ((Integer) var).intValue();
            try
            {
                initOld(ps);
            }
            catch (IOException e)
            {
                System.err.println(e);
                throw new IllegalStateException("initOld failed with IOException");
            }
        }
        else
        {
            try
            {
                initNew(ps);
            }
            catch (IOException e)
            {
                System.err.println(e);
                throw new IllegalStateException("initNew failed with IOException");
            }
            Integer i = new Integer(headerID);
            ps.setProperty("IndexIdentifier", i);
        }
    }

    /**
     * 调用insertData_impl进行插入
     * @param data
     * @param shape
     * @param id
     */
    public void insertData(final byte[] data, final IShape shape, int id)
    {
        if (shape.getDimension() != dimension) throw new IllegalArgumentException("insertData: Shape has the wrong number of dimensions.");

        rwLock.writeLock();

        try
        {
            Region mbr = shape.getMBR();

            byte[] buffer = null;

            if (data != null && data.length > 0)
            {
                buffer = new byte[data.length];
                System.arraycopy(data, 0, buffer, 0, data.length);
            }

            insertData_impl(buffer, mbr, id);
            // the buffer is stored in the tree. Do not delete here.
        }
        finally
        {
            rwLock.writeUnlock();
        }
    }

    /**
     * 调用deleteData_impl进行删除
     * @param shape
     * @param id
     * @return
     */
    public boolean deleteData(final IShape shape, int id)
    {
        if (shape.getDimension() != dimension) throw new IllegalArgumentException("deleteData: Shape has the wrong number of dimensions.");

        rwLock.writeLock();

        try
        {
            Region mbr = shape.getMBR();
            return deleteData_impl(mbr, id);
        }
        finally
        {
            rwLock.writeUnlock();
        }
    }

    /**
     * 查询是否包含
     * @param query
     */
//    public void containmentQuery(final IShape query, final IVisitor v)
    public void containmentQuery(final IShape query)

    {
        if (query.getDimension() != dimension) throw new IllegalArgumentException("containmentQuery: Shape has the wrong number of dimensions.");
//        rangeQuery(SpatialIndex.ContainmentQuery, query, v);
        rangeQuery(SpatialIndex.ContainmentQuery, query);
    }

    public void keywordQuery(final IShape query,byte[] word){

    }

    /**
     * 查询是否交叉
     * @param query
     */
    public void intersectionQuery(final IShape query)
    //    public void intersectionQuery(final IShape query, final IVisitor v)
    {
        if (query.getDimension() != dimension) throw new IllegalArgumentException("intersectionQuery: Shape has the wrong number of dimensions.");
//        rangeQuery(SpatialIndex.IntersectionQuery, query, v);
        rangeQuery(SpatialIndex.IntersectionQuery, query);

    }

    public void pointLocationQuery(final IShape query, final IVisitor v)
    {
        if (query.getDimension() != dimension) throw new IllegalArgumentException("pointLocationQuery: Shape has the wrong number of dimensions.");

        Region r = null;
        if (query instanceof Point)
        {
            r = new Region((Point) query, (Point) query);
        }
        else if (query instanceof Region)
        {
            r = (Region) query;
        }
        else
        {
            throw new IllegalArgumentException("pointLocationQuery: IShape can be Point or Region only.");
        }
        rangeQuery(SpatialIndex.IntersectionQuery, r);
//        rangeQuery(SpatialIndex.IntersectionQuery, r, v);
    }

    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v, final INearestNeighborComparator nnc)
    {
        if (query.getDimension() != dimension) throw new IllegalArgumentException("nearestNeighborQuery: Shape has the wrong number of dimensions.");

        rwLock.readLock();

        try
        {
            // I need a priority queue here. It turns out that TreeSet sorts unique keys only and since I am
            // sorting according to distances, it is not assured that all distances will be unique. TreeMap
            // also sorts unique keys. Thus, I am simulating a priority queue using an ArrayList and binarySearch.
            ArrayList queue = new ArrayList();

            Node n = readNode(rootID);
            queue.add(new NNEntry(n, 0.0));

            int count = 0;
            double knearest = 0.0;

            while (queue.size() != 0)
            {
                NNEntry first = (NNEntry) queue.remove(0);

                if (first.pEntry instanceof Node)
                {
                    n = (Node) first.pEntry;
//					v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.children; cChild++)
                    {
                        IEntry e;

                        if (n.level == 0)
                        {
                            e = new Data(n.pData[cChild], n.pMBR[cChild], n.pIdentifier[cChild]);
                        }
                        else
                        {
                            e = (IEntry) readNode(n.pIdentifier[cChild]);
                        }
                        NNEntry e2 = new NNEntry(e, nnc.getMinimumDistance(query, e));
                        // Why don't I use a TreeSet here? See comment above...
                        int loc = Collections.binarySearch(queue, e2, new NNEntryComparator());
                        if (loc >= 0)
                        {
                            queue.add(loc, e2);
                        }
                        else
                        {
                            queue.add((-loc - 1), e2);
                        }
                    }
                }
                else if(first.pEntry instanceof Data)
                {
                    // report all nearest neighbors with equal furthest distances.
                    // (neighbors can be more than k, if many happen to have the same
                    //  furthest distance).
                    if (count >= k && first.minDist > knearest) break;
                    v.visitData((IData)first.pEntry);
                    stats.queryResults++;
                    count++;
                    knearest = first.minDist;
                }
            }
        }
        finally
        {
            rwLock.readUnlock();
        }
    }

    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v)
    {
        if (query.getDimension() != dimension) throw new IllegalArgumentException("nearestNeighborQuery: Shape has the wrong number of dimensions.");
        NNComparator nnc = new NNComparator();
        nearestNeighborQuery(k, query, v, nnc);
    }

    public void queryStrategy(final IQueryStrategy qs)
    {
        rwLock.readLock();

        int[] next = new int[] {rootID};

        try
        {
            while (true)
            {
                Node n = readNode(next[0]);
                boolean[] hasNext = new boolean[] {false};
                qs.getNextEntry(n, next, hasNext);
                if (hasNext[0] == false) break;
            }
        }
        finally
        {
            rwLock.readUnlock();
        }
    }

    public PropertySet getIndexProperties()
    {
        PropertySet pRet = new PropertySet();

        // dimension
        pRet.setProperty("Dimension", new Integer(dimension));

        // index capacity
        pRet.setProperty("IndexCapacity", new Integer(indexCapacity));

        // leaf capacity
        pRet.setProperty("LeafCapacity", new Integer(leafCapacity));

        // R-tree variant
        pRet.setProperty("TreeVariant", new Integer(treeVariant));

        // fill factor
        pRet.setProperty("FillFactor", new Double(fillFactor));

        // near minimum overlap factor
        pRet.setProperty("NearMinimumOverlapFactor", new Integer(nearMinimumOverlapFactor));

        // split distribution factor
        pRet.setProperty("SplitDistributionFactor", new Double(splitDistributionFactor));

        // reinsert factor
        pRet.setProperty("ReinsertFactor", new Double(reinsertFactor));

        return pRet;
    }

    public void addWriteNodeCommand(INodeCommand nc)
    {
        writeNodeCommands.add(nc);
    }

    public void addReadNodeCommand(INodeCommand nc)
    {
        readNodeCommands.add(nc);
    }

    public void addDeleteNodeCommand(INodeCommand nc)
    {
        deleteNodeCommands.add(nc);
    }

    public boolean isIndexValid()
    {
        boolean ret = true;
        Stack st = new Stack();
        Node root = readNode(rootID);

        if (root.level != stats.treeHeight - 1)
        {
            System.err.println("Invalid tree height");
            return false;
        }

        HashMap nodesInLevel = new HashMap();
        nodesInLevel.put(new Integer(root.level), new Integer(1));

        ValidateEntry e = new ValidateEntry(root.nodeMBR, root);
        st.push(e);

        while (! st.empty())
        {
            e = (ValidateEntry) st.pop();

            Region tmpRegion = (Region) infiniteRegion.clone();

            for (int cDim = 0; cDim < dimension; cDim++)
            {
                tmpRegion.pLow[cDim] = Double.POSITIVE_INFINITY;
                tmpRegion.pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < e.pNode.children; cChild++)
                {
                    tmpRegion.pLow[cDim] = Math.min(tmpRegion.pLow[cDim], e.pNode.pMBR[cChild].pLow[cDim]);
                    tmpRegion.pHigh[cDim] = Math.max(tmpRegion.pHigh[cDim], e.pNode.pMBR[cChild].pHigh[cDim]);
                }
            }

            if (! (tmpRegion.equals(e.pNode.nodeMBR)))
            {
                System.err.println("Invalid parent information");
                ret = false;
            }
            else if (! (tmpRegion.equals(e.parentMBR)))
            {
                System.err.println("Error in parent");
                ret = false;
            }

            if (e.pNode.level != 0)
            {
                for (int cChild = 0; cChild < e.pNode.children; cChild++)
                {
                    ValidateEntry tmpEntry = new ValidateEntry(e.pNode.pMBR[cChild], readNode(e.pNode.pIdentifier[cChild]));

                    if (! nodesInLevel.containsKey(new Integer(tmpEntry.pNode.level)))
                    {
                        nodesInLevel.put(new Integer(tmpEntry.pNode.level), new Integer(1));
                    }
                    else
                    {
                        int i = ((Integer) nodesInLevel.get(new Integer(tmpEntry.pNode.level))).intValue();
                        nodesInLevel.put(new Integer(tmpEntry.pNode.level), new Integer(i + 1));
                    }

                    st.push(tmpEntry);
                }
            }
        }

        int nodes = 0;
        for (int cLevel = 0; cLevel < stats.treeHeight; cLevel++)
        {
            int i1 = ((Integer) nodesInLevel.get(new Integer(cLevel))).intValue();
            int i2 = ((Integer) stats.nodesInLevel.get(cLevel)).intValue();
            if (i1 != i2)
            {
                System.err.println("Invalid nodesInLevel information");
                ret = false;
            }

            nodes += i2;
        }

        if (nodes != stats.nodes)
        {
            System.err.println("Invalid number of nodes information");
            ret = false;
        }

        return ret;
    }

    public IStatistics getStatistics()
    {
        return (IStatistics) stats.clone();
    }

    public void flush() throws IllegalStateException
    {
        try
        {
            storeHeader();
            pStorageManager.flush();
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("flush failed with IOException");
        }
    }

    private void initNew(PropertySet ps) throws IOException
    {
        Object var;

        // tree variant.
        var = ps.getProperty("TreeVariant");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i != SpatialIndex.RtreeVariantLinear &&  i != SpatialIndex.RtreeVariantQuadratic && i != SpatialIndex.RtreeVariantRstar)
                    throw new IllegalArgumentException("Property TreeVariant not a valid variant");
                treeVariant = i;
            }
            else
            {
                throw new IllegalArgumentException("Property TreeVariant must be an Integer");
            }
        }

        // fill factor.
        var = ps.getProperty("FillFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property FillFactor must be in (0.0, 1.0)");
                fillFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property FillFactor must be a Double");
            }
        }

        // index capacity.
        var = ps.getProperty("IndexCapacity");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 3) throw new IllegalArgumentException("Property IndexCapacity must be >= 3");
                indexCapacity = i;
            }
            else
            {
                throw new IllegalArgumentException("Property IndexCapacity must be an Integer");
            }
        }

        // leaf capacity.
        var = ps.getProperty("LeafCapacity");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 3) throw new IllegalArgumentException("Property LeafCapacity must be >= 3");
                leafCapacity = i;
            }
            else
            {
                throw new IllegalArgumentException("Property LeafCapacity must be an Integer");
            }
        }

        // near minimum overlap factor.
        var = ps.getProperty("NearMinimumOverlapFactor");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 1 || i > indexCapacity || i > leafCapacity)
                    throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be less than both index and leaf capacities");
                nearMinimumOverlapFactor = i;
            }
            else
            {
                throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be an Integer");
            }
        }

        // split distribution factor.
        var = ps.getProperty("SplitDistributionFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property SplitDistributionFactor must be in (0.0, 1.0)");
                splitDistributionFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property SplitDistriburionFactor must be a Double");
            }
        }

        // reinsert factor.
        var = ps.getProperty("ReinsertFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property ReinsertFactor must be in (0.0, 1.0)");
                reinsertFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property ReinsertFactor must be a Double");
            }
        }

        // dimension
        var = ps.getProperty("Dimension");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i <= 1) throw new IllegalArgumentException("Property Dimension must be >= 1");
                dimension = i;
            }
            else
            {
                throw new IllegalArgumentException("Property Dimension must be an Integer");
            }
        }

        infiniteRegion.pLow = new double[dimension];
        infiniteRegion.pHigh = new double[dimension];

        for (int cDim = 0; cDim < dimension; cDim++)
        {
            infiniteRegion.pLow[cDim] = Double.POSITIVE_INFINITY;
            infiniteRegion.pHigh[cDim] = Double.NEGATIVE_INFINITY;
        }

        stats.treeHeight = 1;
        stats.nodesInLevel.add(new Integer(0));

        Leaf root = new Leaf(this, -1);
        rootID = writeNode(root);

        storeHeader();
    }

    private void initOld(PropertySet ps) throws IOException
    {
        loadHeader();

        // only some of the properties may be changed.
        // the rest are just ignored.

        Object var;

        // tree variant.
        var = ps.getProperty("TreeVariant");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i != SpatialIndex.RtreeVariantLinear &&  i != SpatialIndex.RtreeVariantQuadratic && i != SpatialIndex.RtreeVariantRstar)
                    throw new IllegalArgumentException("Property TreeVariant not a valid variant");
                treeVariant = i;
            }
            else
            {
                throw new IllegalArgumentException("Property TreeVariant must be an Integer");
            }
        }

        // near minimum overlap factor.
        var = ps.getProperty("NearMinimumOverlapFactor");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 1 || i > indexCapacity || i > leafCapacity)
                    throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be less than both index and leaf capacities");
                nearMinimumOverlapFactor = i;
            }
            else
            {
                throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be an Integer");
            }
        }

        // split distribution factor.
        var = ps.getProperty("SplitDistributionFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property SplitDistributionFactor must be in (0.0, 1.0)");
                splitDistributionFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property SplitDistriburionFactor must be a Double");
            }
        }

        // reinsert factor.
        var = ps.getProperty("ReinsertFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property ReinsertFactor must be in (0.0, 1.0)");
                reinsertFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property ReinsertFactor must be a Double");
            }
        }

        infiniteRegion.pLow = new double[dimension];
        infiniteRegion.pHigh = new double[dimension];

        for (int cDim = 0; cDim < dimension; cDim++)
        {
            infiniteRegion.pLow[cDim] = Double.POSITIVE_INFINITY;
            infiniteRegion.pHigh[cDim] = Double.NEGATIVE_INFINITY;
        }
    }

    private void storeHeader() throws IOException
    {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);

        ds.writeInt(rootID);
        ds.writeInt(treeVariant);
        ds.writeDouble(fillFactor);
        ds.writeInt(indexCapacity);
        ds.writeInt(leafCapacity);
        ds.writeInt(nearMinimumOverlapFactor);
        ds.writeDouble(splitDistributionFactor);
        ds.writeDouble(reinsertFactor);
        ds.writeInt(dimension);
        ds.writeLong(stats.nodes);
        ds.writeLong(stats.data);
        ds.writeInt(stats.treeHeight);

        for (int cLevel = 0; cLevel < stats.treeHeight; cLevel++)
        {
            ds.writeInt(((Integer) stats.nodesInLevel.get(cLevel)).intValue());
        }

        ds.flush();
        headerID = pStorageManager.storeByteArray(headerID, bs.toByteArray());
    }

    private void loadHeader() throws IOException
    {
        byte[] data = pStorageManager.loadByteArray(headerID);
        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));

        rootID = ds.readInt();
        treeVariant = ds.readInt();
        fillFactor = ds.readDouble();
        indexCapacity = ds.readInt();
        leafCapacity = ds.readInt();
        nearMinimumOverlapFactor = ds.readInt();
        splitDistributionFactor = ds.readDouble();
        reinsertFactor = ds.readDouble();
        dimension = ds.readInt();
        stats.nodes = ds.readLong();
        stats.data = ds.readLong();
        stats.treeHeight = ds.readInt();

        for (int cLevel = 0; cLevel < stats.treeHeight; cLevel++)
        {
            stats.nodesInLevel.add(new Integer(ds.readInt()));
        }
    }

    /**
     * 调用chooseSubtree选择合适的子树插入MBR
     * @param pData
     * @param mbr
     * @param id
     */
    protected void insertData_impl(byte[] pData, Region mbr, int id)
    {
//		assert mbr.getDimension() == (int)m_dimension;

        boolean[] overflowTable;

        Stack pathBuffer = new Stack();

        Node root = readNode(rootID);

        overflowTable = new boolean[root.level];
        for (int cLevel = 0; cLevel < root.level; cLevel++) overflowTable[cLevel] = false;

        Node l = root.chooseSubtree(mbr, 0, pathBuffer);
        l.insertData(pData, mbr, id, pathBuffer, overflowTable);

        stats.data++;
    }

    protected void insertData_impl(byte[] pData, Region mbr, int id, int level, boolean[] overflowTable)
    {
//		assert mbr.getDimension() == m_dimension;

        Stack pathBuffer = new Stack();

        Node root = readNode(rootID);
        Node n = root.chooseSubtree(mbr, level, pathBuffer);
        n.insertData(pData, mbr, id, pathBuffer, overflowTable);
    }

    /**
     * 调用findLeaf找到MBR所在的叶节点进行删除
     * @param mbr
     * @param id
     * @return
     */
    protected boolean deleteData_impl(final Region mbr, int id)
    {
//		assert mbr.getDimension() == m_dimension;

        boolean bRet = false;

        Stack pathBuffer = new Stack();

        Node root = readNode(rootID);
        Leaf l = root.findLeaf(mbr, id, pathBuffer);

        if (l != null)
        {
            l.deleteData(id, pathBuffer);
            stats.data--;
            bRet = true;
        }

        return bRet;
    }

    protected int writeNode(Node n) throws IllegalStateException
    {
        byte[] buffer = null;

        try
        {
            buffer = n.store();
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("writeNode failed with IOException");
        }

        int page;
        if (n.identifier < 0) page = IStorageManager.NewPage;
        else page = n.identifier;

        try
        {
            page = pStorageManager.storeByteArray(page, buffer);
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("writeNode failed with InvalidPageException");
        }

        if (n.identifier < 0)
        {
            n.identifier = page;
            stats.nodes++;
            int i = ((Integer) stats.nodesInLevel.get(n.level)).intValue();
            stats.nodesInLevel.set(n.level, new Integer(i + 1));
        }

        stats.writes++;

        for (int cIndex = 0; cIndex < writeNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) writeNodeCommands.get(cIndex)).execute(n);
        }

        return page;
    }

    public Node readNode(int id)
    {
        byte[] buffer;
        DataInputStream ds = null;
        int nodeType = -1;
        Node n = null;

        try
        {
            buffer = pStorageManager.loadByteArray(id);
            ds = new DataInputStream(new ByteArrayInputStream(buffer));
            nodeType = ds.readInt();

            if (nodeType == SpatialIndex.PersistentIndex) n = new Index(this, -1, 0);
            else if (nodeType == SpatialIndex.PersistentLeaf) n = new Leaf(this, -1);
            else throw new IllegalStateException("readNode failed reading the correct node type information");

            n.pTree = this;
            n.identifier = id;
            n.load(buffer);

            stats.reads++;
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("readNode failed with InvalidPageException");
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("readNode failed with IOException");
        }

        for (int cIndex = 0; cIndex < readNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) readNodeCommands.get(cIndex)).execute(n);
        }

        return n;
    }

    protected void deleteNode(Node n)
    {
        try
        {
            pStorageManager.deleteByteArray(n.identifier);
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("deleteNode failed with InvalidPageException");
        }

        stats.nodes--;
        int i = ((Integer) stats.nodesInLevel.get(n.level)).intValue();
        stats.nodesInLevel.set(n.level, new Integer(i - 1));

        for (int cIndex = 0; cIndex < deleteNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) deleteNodeCommands.get(cIndex)).execute(n);
        }
    }

//    private void rangeQuery(int type, final IShape query, final IVisitor v)
    private void rangeQuery(int type,final IShape query)
    {
        rwLock.readLock();

        try
        {
            Stack st = new Stack();
            Node root = readNode(rootID);


            if (root.children > 0 && query.intersects(root.nodeMBR)) st.push(root);

            while (! st.empty())
            {
                Node n = (Node) st.pop();

                if (n.level == 0)
                {
//                    v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.children; cChild++)
                    {
                        boolean b;
                        if (type == SpatialIndex.ContainmentQuery) b = query.contains(n.pMBR[cChild]);
                        else b = query.intersects(n.pMBR[cChild]);

                        if (b)
                        {
//							Data data = new Data(n.m_pData[cChild], n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
//							v.visitData(data);
                            stats.queryResults++;
                        }
                    }
                }
                else
                {
//                    v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.children; cChild++)
                    {
                        if (query.intersects(n.pMBR[cChild]))
                        {
                            st.push(readNode(n.pIdentifier[cChild]));
                            System.out.println("in "+n.pMBR[cChild].toString());
                        }
                    }
                }
            }
        }
        finally
        {
            rwLock.readUnlock();
        }
    }

    public String toString()
    {
        String s = "Dimension: " + dimension + "\n"
                + "Fill factor: " + fillFactor + "\n"
                + "Index capacity: " + indexCapacity + "\n"
                + "Leaf capacity: " + leafCapacity + "\n";

        if (treeVariant == SpatialIndex.RtreeVariantRstar)
        {
            s += "Near minimum overlap factor: " + nearMinimumOverlapFactor + "\n"
                    + "Reinsert factor: " + reinsertFactor + "\n"
                    + "Split distribution factor: " + splitDistributionFactor + "\n";
        }

        s += "Utilization: " + 100 * stats.getNumberOfData() / (stats.getNumberOfNodesInLevel(0) * leafCapacity) + "%" + "\n"
                + stats;

        return s;
    }


    class NNEntry
    {
        IEntry pEntry;
        double minDist;

        NNEntry(IEntry e, double f) { pEntry = e; minDist = f; }
    }

    class NNEntryComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            NNEntry n1 = (NNEntry) o1;
            NNEntry n2 = (NNEntry) o2;

            if (n1.minDist < n2.minDist) return -1;
            if (n1.minDist > n2.minDist) return 1;
            return 0;
        }
    }

    class NNComparator implements INearestNeighborComparator
    {
        public double getMinimumDistance(IShape query, IEntry e)
        {
            IShape s = e.getShape();
            return query.getMinimumDistance(s);
        }
    }

    class ValidateEntry
    {
        Region parentMBR;
        Node pNode;

        ValidateEntry(Region r, Node pNode) { parentMBR = r; pNode = pNode; }
    }

    class Data implements IData
    {
        int id;
        Region shape;
        byte[] pData;

        Data(byte[] pData, Region mbr, int id) { this.id = id; shape = mbr; this.pData = pData; }

        public int getIdentifier() { return id; }
        public IShape getShape() { return new Region(shape); }
        public byte[] getData()
        {
            byte[] data = new byte[this.pData.length];
            System.arraycopy(this.pData, 0, data, 0, this.pData.length);
            return data;
        }
    }
}
