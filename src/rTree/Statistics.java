package rTree;

import spatialIndex.IStatistics;

import java.util.ArrayList;

public class Statistics implements IStatistics,Cloneable {
    protected long reads;
    protected long writes;
    protected long splits;
    protected long hits;
    protected long misses;
    protected long nodes;
    protected long adjustments;
    protected long queryResults;
    protected long data;
    protected int treeHeight;
    protected ArrayList nodesInLevel = new ArrayList();

    public Statistics()
    {
        reset();
    }

    public Statistics(Statistics s)
    {
        reads  = s.reads;
        writes = s.writes;
        splits = s.splits;
        hits   = s.hits;
        misses = s.misses;
        nodes  = s.nodes;
        adjustments = s.adjustments;
        queryResults = s.queryResults;
        data = s.data;
        treeHeight = s.treeHeight;
        nodesInLevel = (ArrayList) s.nodesInLevel.clone();
    }

    public long getReads()
    {
        return reads;
    }

    public long getWrites()
    {
        return writes;
    }

    public long getNumberOfNodes()
    {
        return nodes;
    }

    public long getNumberOfData()
    {
        return data;
    }

    public long getSplits()
    {
        return splits;
    }

    public long getHits()
    {
        return hits;
    }

    public long getMisses()
    {
        return misses;
    }

    public long getAdjustments()
    {
        return adjustments;
    }

    public long getQueryResults()
    {
        return queryResults;
    }

    public int getTreeHeight()
    {
        return treeHeight;
    }

    public int getNumberOfNodesInLevel(int l) throws IndexOutOfBoundsException
    {
        return ((Integer) nodesInLevel.get(l)).intValue();
    }

    public void reset()
    {
        reads  = 0;
        writes = 0;
        splits = 0;
        hits   = 0;
        misses = 0;
        nodes  = 0;
        adjustments = 0;
        queryResults = 0;
        data = 0;
        treeHeight = 0;
        nodesInLevel.clear();
    }

    public String toString()
    {
        String s = "Reads: " + reads + "\n" +
                "Writes: " + writes + "\n" +
                "Hits: " + hits + "\n" +
                "Misses: " + misses + "\n" +
                "Tree height: " + treeHeight + "\n" +
                "Number of data: " + data + "\n" +
                "Number of nodes: " + nodes + "\n";

        for (int cLevel = 0; cLevel < treeHeight; cLevel++)
        {
            s += "Level " + cLevel + " pages: " + ((Integer) nodesInLevel.get(cLevel)).intValue() + "\n";
        }

        s += "Splits: " + splits + "\n" +
                "Adjustments: " + adjustments + "\n" +
                "Query results: " + queryResults;

        return s;
    }

    public Object clone()
    {
        return new Statistics(this);
    }
}
