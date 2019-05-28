package storageManager;

import java.io.*;
import java.util.*;

public class DiskStorageManager implements IStorageManager{
    private RandomAccessFile dataFile = null;
    private RandomAccessFile indexFile = null;
    private int pageSize = 0;
    private int nextPage = -1;
    private TreeSet emptyPages = new TreeSet();
    private HashMap pageIndex = new HashMap();
    private byte[] buffer = null;

    public DiskStorageManager(PropertySet ps)
            throws SecurityException, NullPointerException, IOException, FileNotFoundException, IllegalArgumentException
    {
        Object var;

        // Open/Create flag.
        boolean bOverwrite = false;
        var = ps.getProperty("Overwrite");

        if (var != null)
        {
            if (! (var instanceof Boolean)) throw new IllegalArgumentException("Property Overwrite must be a Boolean");
            bOverwrite = ((Boolean) var).booleanValue();
        }

        // storage filename.
        var = ps.getProperty("FileName");

        if (var != null)
        {
            if (! (var instanceof String)) throw new IllegalArgumentException("Property FileName must be a String");

            File indexFile = new File((String) var + ".idx");
            File dataFile = new File((String) var + ".dat");
//            flush();
            // check if files exist.
            if (bOverwrite == false)// && (! indexFile.exists() || ! dataFile.exists()))
                bOverwrite = true;

            if (bOverwrite)
            {
                if (indexFile.exists()) indexFile.delete();
                if (dataFile.exists()) dataFile.delete();

                boolean b = indexFile.createNewFile();
                if (b == false) throw new IOException("Index file cannot be opened.");

                b = dataFile.createNewFile();
                if (b == false) throw new IOException("Data file cannot be opened.");
            }

            this.indexFile = new RandomAccessFile(indexFile, "rw");
            this.dataFile = new RandomAccessFile(dataFile, "rw");
        }
        else
        {
            throw new IllegalArgumentException("Property FileName was not specified.");
        }

        // find page size.
        if (bOverwrite == true)
        {
            var = ps.getProperty("PageSize");

            if (var != null)
            {
                if (! (var instanceof Integer)) throw new IllegalArgumentException("Property PageSize must be an Integer");
                this.pageSize = ((Integer) var).intValue();
                this.nextPage = 0;
            }
            else
            {
                throw new IllegalArgumentException("Property PageSize was not specified.");
            }
        }
        else
        {
            try
            {
                this.pageSize = this.indexFile.readInt();
            }
            catch (EOFException ex)
            {
                throw new IllegalStateException("Failed reading pageSize.");
            }

            try
            {
                this.nextPage = this.indexFile.readInt();
            }
            catch (EOFException ex)
            {
                throw new IllegalStateException("Failed reading nextPage.");
            }
        }

        // create buffer.
        this.buffer = new byte[this.pageSize];

        if (bOverwrite == false)
        {
            int count, id, page;

            // load empty pages in memory.
            try
            {
                count = this.indexFile.readInt();

                for (int cCount = 0; cCount < count; cCount++)
                {
                    page = this.indexFile.readInt();
                    this.emptyPages.add(new Integer(page));
                }

                // load index table in memory.
                count = this.indexFile.readInt();

                for (int cCount = 0; cCount < count; cCount++)
                {
                    Entry e = new Entry();

                    id = this.indexFile.readInt();
                    e.length = this.indexFile.readInt();

                    int count2 = indexFile.readInt();

                    for (int cCount2 = 0; cCount2 < count2; cCount2++)
                    {
                        page = indexFile.readInt();
                        e.pages.add(new Integer(page));
                    }
                    this.pageIndex.put(new Integer(id), e);
                }
            }
            catch (EOFException ex)
            {
                throw new IllegalStateException("Corrupted index file.");
            }
        }
    }

    public void flush()
    {
        try
        {
            this.indexFile.seek(0l);

            this.indexFile.writeInt(this.pageSize);
            this.indexFile.writeInt(this.nextPage);

            int id, page;
            int count = this.emptyPages.size();

            this.indexFile.writeInt(count);

            Iterator it = this.emptyPages.iterator();
            while (it.hasNext())
            {
                page = ((Integer) it.next()).intValue();
                this.indexFile.writeInt(page);
            }

            count = this.pageIndex.size();
            this.indexFile.writeInt(count);

            it = this.pageIndex.entrySet().iterator();

            while (it.hasNext())
            {
                Map.Entry me = (Map.Entry) it.next();
                id = ((Integer) me.getKey()).intValue();
                this.indexFile.writeInt(id);

                Entry e = (Entry) me.getValue();
                count = e.length;
                this.indexFile.writeInt(count);

                count = e.pages.size();
                this.indexFile.writeInt(count);

                for (int cIndex = 0; cIndex < count; cIndex++)
                {
                    page = ((Integer) e.pages.get(cIndex)).intValue();
                    this.indexFile.writeInt(page);
                }
            }
        }
        catch (IOException ex)
        {
            throw new IllegalStateException("Corrupted index file.");
        }
    }

    public byte[] loadByteArray(final int id)
    {
        Entry e = (Entry) this.pageIndex.get(new Integer(id));
        if (e == null) throw new InvalidPageException(id);

        int cNext = 0;
        int cTotal = e.pages.size();

        byte[] data = new byte[e.length];
        int cIndex = 0;
        int cLen;
        int cRem = e.length;

        do
        {
            try
            {
                this.dataFile.seek(((Integer) e.pages.get(cNext)).intValue() * this.pageSize);
                int bytesread = this.dataFile.read(this.buffer);
                if (bytesread != this.pageSize) throw new IllegalStateException("Corrupted data file.");
            }
            catch (IOException ex)
            {
                throw new IllegalStateException("Corrupted data file.");
            }

            cLen = (cRem > this.pageSize) ? this.pageSize : cRem;
            System.arraycopy(this.buffer, 0, data, cIndex, cLen);

            cIndex += cLen;
            cRem -= cLen;
            cNext++;
        }
        while (cNext < cTotal);

        return data;
    }

    public int storeByteArray(final int id, final byte[] data)
    {
        if (id == NewPage)
        {
            Entry e = new Entry();
            e.length = data.length;

            int cIndex = 0;
            int cPage;
            int cRem = data.length;
            int cLen;

            while (cRem > 0)
            {
                if (! this.emptyPages.isEmpty())
                {
                    Integer i = (Integer) this.emptyPages.first();
                    this.emptyPages.remove(i);
                    cPage = i.intValue();
                }
                else
                {
                    cPage = this.nextPage;
                    this.nextPage++;
                }

                cLen = (cRem > this.pageSize) ? this.pageSize : cRem;
                System.arraycopy(data, cIndex, this.buffer, 0, cLen);

                try
                {
                    this.dataFile.seek(cPage * this.pageSize);
                    this.dataFile.write(this.buffer);
                }
                catch (IOException ex)
                {
                    throw new IllegalStateException("Corrupted data file.");
                }

                cIndex += cLen;
                cRem -= cLen;
                e.pages.add(new Integer(cPage));
            }

            Integer i = (Integer) e.pages.get(0);
            this.pageIndex.put(i, e);

            return i.intValue();
        }
        else
        {
            // find the entry.
            Entry oldEntry = (Entry) this.pageIndex.get(new Integer(id));
            if (oldEntry == null) throw new InvalidPageException(id);

            this.pageIndex.remove(new Integer(id));

            Entry e = new Entry();
            e.length = data.length;

            int cIndex = 0;
            int cPage;
            int cRem = data.length;
            int cLen, cNext = 0;

            while (cRem > 0)
            {
                if (cNext < oldEntry.pages.size())
                {
                    cPage = ((Integer) oldEntry.pages.get(cNext)).intValue();
                    cNext++;
                }
                else if (! this.emptyPages.isEmpty())
                {
                    Integer i = (Integer) this.emptyPages.first();
                    this.emptyPages.remove(i);
                    cPage = i.intValue();
                }
                else
                {
                    cPage = this.nextPage;
                    this.nextPage++;
                }

                cLen = (cRem > this.pageSize) ? this.pageSize : cRem;
                System.arraycopy(data, cIndex, this.buffer, 0, cLen);

                try
                {
                    this.dataFile.seek(cPage * this.pageSize);
                    this.dataFile.write(this.buffer);
                }
                catch (IOException ex)
                {
                    throw new IllegalStateException("Corrupted data file.");
                }

                cIndex += cLen;
                cRem -= cLen;
                e.pages.add(new Integer(cPage));
            }

            while (cNext < oldEntry.pages.size())
            {
                this.emptyPages.add(oldEntry.pages.get(cNext));
                cNext++;
            }

            Integer i = (Integer) e.pages.get(0);
            this.pageIndex.put(i, e);

            return i.intValue();
        }
    }

    public void deleteByteArray(final int id)
    {
        // find the entry.
        Entry e = (Entry) this.pageIndex.get(new Integer(id));
        if (e == null) throw new InvalidPageException(id);

        this.pageIndex.remove(new Integer(id));

        for (int cIndex = 0; cIndex < e.pages.size(); cIndex++)
        {
            this.emptyPages.add(e.pages.get(cIndex));
        }
    }

    public void close()
    {
        flush();
    }

    class Entry
    {
        int length = 0;
        ArrayList pages = new ArrayList();
    }
}
