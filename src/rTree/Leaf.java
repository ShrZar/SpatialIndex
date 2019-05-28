package rTree;

import spatialIndex.Region;

import java.util.ArrayList;
import java.util.Stack;

public class Leaf extends Node {
    public Leaf(RTree pTree, int id)
    {
        super(pTree, id, 0, pTree.leafCapacity);
    }

    protected Node chooseSubtree(Region mbr, int level, Stack pathBuffer)
    {
        return this;
    }

    protected Leaf findLeaf(Region mbr, int id, Stack pathBuffer)
    {
        for(int cChild = 0; cChild < children; cChild++)
            if(pIdentifier[cChild] == id && mbr.equals(pMBR[cChild]))
                return this;

        return null;
    }

    protected void deleteData(int id, Stack pathBuffer)
    {
        int child;
        for(child = 0; child < children; child++)
            if(pIdentifier[child] == id)
                break;

        deleteEntry(child);
        pTree.writeNode(this);
        Stack toReinsert = new Stack();
        condenseTree(toReinsert, pathBuffer);
        while(!toReinsert.empty())
        {
            Node n = (Node)toReinsert.pop();
            pTree.deleteNode(n);
            for(int cChild = 0; cChild < n.children; cChild++)
            {
                boolean overflowTable[] = new boolean[pTree.stats.treeHeight];
                for(int cLevel = 0; cLevel < pTree.stats.treeHeight; cLevel++)
                    overflowTable[cLevel] = false;

                pTree.insertData_impl(n.pData[cChild], n.pMBR[cChild], n.pIdentifier[cChild], n.level, overflowTable);
                n.pData[cChild] = null;
            }
        }
    }

    protected Node[] split(byte pData[], Region mbr, int id)
    {
        pTree.stats.splits++;
        ArrayList g1 = new ArrayList();
        ArrayList g2 = new ArrayList();
        switch(pTree.treeVariant)
        {
            case 1: // '\001'
            case 2: // '\002'
                rtreeSplit(pData, mbr, id, g1, g2);
                break;

            case 3: // '\003'
                rstarSplit(pData, mbr, id, g1, g2);
                break;

            default:
                throw new IllegalStateException("Unknown RTree variant.");
        }
        Node left = new Leaf(pTree, -1);
        Node right = new Leaf(pTree, -1);
        for(int cIndex = 0; cIndex < g1.size(); cIndex++)
        {
            int i = ((Integer)g1.get(cIndex)).intValue();
            left.insertEntry(this.pData[i], pMBR[i], pIdentifier[i]);
            this.pData[i] = null;
        }

        for(int cIndex = 0; cIndex < g2.size(); cIndex++)
        {
            int i = ((Integer)g2.get(cIndex)).intValue();
            right.insertEntry(this.pData[i], pMBR[i], pIdentifier[i]);
            this.pData[i] = null;
        }

        Node ret[] = new Node[2];
        ret[0] = left;
        ret[1] = right;
        return ret;
    }

    public String getDataAsString(int id)
    {
        int length = pDataLength[id];
        byte data[] = new byte[length];
        for(int i = 0; i < data.length; i++)
            data[i] = pData[id][i];

        String value = new String(data);
        return value;
    }
}
