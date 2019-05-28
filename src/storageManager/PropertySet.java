package storageManager;

import java.util.HashMap;

public class PropertySet {
    private HashMap propertySet = new HashMap();

    public Object getProperty(String property)
    {
        return propertySet.get(property);
    }

    public void setProperty(String property, Object o)
    {
        propertySet.put(property, o);
    }
}
