package spatialIndex;

import java.util.LinkedList;

public class RWLock {
    private int activeReaders;
    private int waitingReaders;
    private int activeWriters;

    private final LinkedList writerLocks = new LinkedList();

    public synchronized void readLock()
    {
        if (activeWriters == 0 && writerLocks.size() == 0)
            ++activeReaders;
        else
        {
            ++waitingReaders;
            try{ wait(); } catch (InterruptedException e) {}
        }
    }

    public synchronized boolean readLockNoblock()
    {
        if (activeWriters == 0 && writerLocks.size() == 0)
        {
            ++activeReaders;
            return true;
        }
        return false;
    }

    public synchronized void readUnlock()
    {
        if (--activeReaders == 0) notifyWriters();
    }

    public void writeLock()
    {
        Object lock = new Object();
        synchronized(lock)
        {
            synchronized(this)
            {
                boolean okayToWrite = writerLocks.size() == 0 && activeReaders == 0 && activeWriters == 0;
                if (okayToWrite)
                {
                    ++activeWriters;
                    return; // the "return" jumps over the "wait" call
                }

                writerLocks.addLast(lock);
            }
            try { lock.wait(); } catch (InterruptedException e) {}
        }
    }

    synchronized public boolean writeLockNoblock()
    {
        if (writerLocks.size() == 0  && activeReaders == 0 && activeWriters == 0)
        {
            ++activeWriters;
            return true;
        }
        return false;
    }

    public synchronized void writeUnlock()
    {
        --activeWriters;
        if (waitingReaders > 0)   // priority to waiting readers
            notifyReaders();
        else
            notifyWriters();
    }

    private void notifyReaders()       // must be accessed from a
    {                                   //  synchronized method
        activeReaders  += waitingReaders;
        waitingReaders = 0;
        notifyAll();
    }

    private void notifyWriters()       // must be accessed from a
    {                                   //  synchronized method
        if (writerLocks.size() > 0)
        {
            Object oldest = writerLocks.removeFirst();
            ++activeWriters;
            synchronized(oldest) { oldest.notify(); }
        }
    }
}
