package spatialIndex;

public interface INode extends IEntry{
    public int getChildrenCount();
    public int getChildIdentifier(int index) throws IndexOutOfBoundsException;
    public IShape getChildShape(int index) throws IndexOutOfBoundsException;
    public int getLevel();
    public boolean isIndex();
    public boolean isLeaf();
}
