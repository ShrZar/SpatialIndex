package storageManager;

import java.util.ArrayList;
import java.util.Stack;

public class MemoryStorageManager implements IStorageManager {
    private ArrayList buffer = new ArrayList();
    private Stack emptyPages = new Stack();

    public void flush()
    {
    }

    public byte[] loadByteArray(final int id)
    {
        Entry e = null;

        try
        {
            e = (Entry) this.buffer.get(id);
        }
        catch (IndexOutOfBoundsException ex)
        {
            throw new InvalidPageException(id);
        }

        byte[] ret = new byte[e.pData.length];
        System.arraycopy(e.pData, 0, ret, 0, e.pData.length);
        return ret;
    }

    public int storeByteArray(final int id, final byte[] data)
    {
        int ret = id;
        Entry e = new Entry(data);

        if (id == NewPage)
        {
            if (this.emptyPages.empty())
            {
                this.buffer.add(e);
                ret = this.buffer.size() - 1;
            }
            else
            {
                ret = ((Integer) this.emptyPages.pop()).intValue();
                this.buffer.set(ret, e);
            }
        }
        else
        {
            if (id < 0 || id >= this.buffer.size()) throw new InvalidPageException(id);
            this.buffer.set(id, e);
        }

        return ret;
    }

    public void deleteByteArray(final int id)
    {
        Entry e = null;
        try
        {
            e = (Entry) this.buffer.get(id);
        }
        catch (IndexOutOfBoundsException ex)
        {
            throw new InvalidPageException(id);
        }

        this.buffer.set(id, null);
        this.emptyPages.push(new Integer(id));
    }

    class Entry
    {
        byte[] pData;

        Entry(final byte[] d)
        {
            this.pData = new byte[d.length];
            System.arraycopy(d, 0, this.pData, 0, d.length);
        }
    } // Entry
}
