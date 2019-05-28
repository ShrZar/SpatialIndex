package spatialIndex;

import storageManager.*;

import java.io.FileNotFoundException;
import java.io.IOException;

public class SpatialIndex {
    public static final double EPSILON = 1.192092896e-07;

    public static final int RtreeVariantQuadratic = 1;
    public static final int RtreeVariantLinear = 2;
    public static final int RtreeVariantRstar = 3;

    public static final int PersistentIndex = 1;
    public static final int PersistentLeaf = 2;

    public static final int ContainmentQuery = 1;
    public static final int IntersectionQuery = 2;

    public static ISpatialIndex createRTree(PropertySet ps, IStorageManager sm)
    {
        return null;
    }

    public static IStorageManager createMemoryStorageManager(PropertySet ps)
    {
        IStorageManager sm = (IStorageManager) new MemoryStorageManager();
        return sm;
    }

    public static IStorageManager createDiskStorageManager(PropertySet ps)
            throws SecurityException, NullPointerException, IOException, FileNotFoundException, IllegalArgumentException
    {
        IStorageManager sm = (IStorageManager) new DiskStorageManager(ps);
        return sm;
    }
}
