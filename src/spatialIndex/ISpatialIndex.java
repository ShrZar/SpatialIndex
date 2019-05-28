package spatialIndex;

import storageManager.PropertySet;

public interface ISpatialIndex {
    public void flush() throws IllegalStateException;
    public void insertData(final byte[] data, final IShape shape, int id);
    public boolean deleteData(final IShape shape, int id);
//    public void containmentQuery(final IShape query, final IVisitor v);
public void containmentQuery(final IShape query);

    //    public void intersectionQuery(final IShape query, final IVisitor v);
    public void intersectionQuery(final IShape query);
    public void pointLocationQuery(final IShape query, final IVisitor v);
    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v, INearestNeighborComparator nnc);
    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v);
    public void queryStrategy(final IQueryStrategy qs);
    public PropertySet getIndexProperties();
    public void addWriteNodeCommand(INodeCommand nc);
    public void addReadNodeCommand(INodeCommand nc);
    public void addDeleteNodeCommand(INodeCommand nc);
    public boolean isIndexValid();
    public IStatistics getStatistics();
}
