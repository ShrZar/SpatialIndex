package spatialIndex;

import spatialIndex.IData;
import spatialIndex.IShape;

public class DataBlock implements IData {
    protected byte[] data;
    public byte[] getData(){
        return this.data;
    }

    @Override
    public int getIdentifier() {
        return 0;
    }

    @Override
    public IShape getShape() {
        return null;
    }
}
