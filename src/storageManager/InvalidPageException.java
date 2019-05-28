package storageManager;

public class InvalidPageException extends RuntimeException {
    public InvalidPageException(int id)
    {
        super("" + id);
    }
}
