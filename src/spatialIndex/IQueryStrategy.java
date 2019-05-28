package spatialIndex;

public interface IQueryStrategy {
    public void getNextEntry(IEntry e, int[] nextEntry, boolean[] hasNext);
}
