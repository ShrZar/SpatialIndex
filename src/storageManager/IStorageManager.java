package storageManager;

public interface IStorageManager {
    public static final int NewPage = -1;

    public void flush();
    public byte[] loadByteArray(final int id);
    public int storeByteArray(final int id, final byte[] data);
    public void deleteByteArray(final int id);
}
