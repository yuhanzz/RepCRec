import java.util.*;

public class DataManager {
    Map<Integer, DataCopy> dataCopies;  // <key : variable id, value : data copy>

    /**
     * initialize the data manager
     */
    public DataManager(int siteId) {
        for (int i = 1; i <= 20; i++) {
            if (i % 2 == 0) {
                dataCopies.put(i, new DataCopy(DataType.REPLICATED, 10 * i));
                continue;
            }
            if ((1 + i % 10) == siteId) {
                dataCopies.put(i, new DataCopy(DataType.NOT_REPLICATED, 10 * i));
            }
        }
    }

    /**
     * return true if this copy is available for read
     */
    public boolean readAvailable(int variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        if(dCopy.isReadAvailable()) return true;
        return false;
    }

    /**
     * read the latest committed value for snapshot purpose
     */
    public int readCommittedValue(Integer variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        return dCopy.getCommittedValue();
    }

    /**
     * update the committed value with its current value
     */
    public void commitVariable(int variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        int currValue = dCopy.getCurrentValue();
        dCopy.setCommittedValue(currValue);
    }

    /**
     * abort the change on this variable
     */
    public void abortVariable(int variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        int committedValue = dCopy.getCommittedValue();
        dCopy.setCurrentValue(committedValue);
    }

    /**
     * write the current value
     */
    public void write(int variableId, int value) {
        DataCopy dCopy = dataCopies.get(variableId);
        dCopy.setCurrentValue(value);
    }

    /**
     * read the current value
     */
    public int read(int variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        return dCopy.getCurrentValue();
    }

    /**
     * Update the readAvailable to what is specified
     */
    public void updateReadAvail(int variableId,boolean avail)
    {
        DataCopy dCopy = dataCopies.get(variableId);
        dCopy.setReadAvailable(avail);
    }


    public Set<Integer> getAllReadAvail()
    {
        Set<Integer> variableIds = new HashSet<>(); 
        for(Integer variable: dataCopies.keySet())
        {
            if(readAvailable(variable))
            {
                variableIds.add(variable);
            }
        }
        return variableIds;
    }
    

    public void setAllNonReplicatedReadAvail(boolean avail)
    {
        Set<Map.Entry<Integer,DataCopy>> set = dataCopies.entrySet();
        for(Map.Entry<Integer,DataCopy> entry: set)
        {
            if (entry.getValue().getDataType() == DataType.NOT_REPLICATED)
            {
                updateReadAvail(entry.getKey(), avail);
            }
        }
    }
}
