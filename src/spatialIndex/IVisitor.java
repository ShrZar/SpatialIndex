package spatialIndex;

public interface IVisitor {
    public void visitNode(final INode n);
    public void visitData(final IData d);
}
