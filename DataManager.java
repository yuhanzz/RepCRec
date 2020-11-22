import java.util.*;

import javax.xml.crypto.Data;

public class DataManager {
    Map<Integer, DataCopy> dataCopies;  // <key : variable id, value : data copy>

    /**
     * return true if this copy is available for read
     */
    public boolean readAvailable(int variableId) {
        
    }

    /**
     * read the latest committed value for snapshot purpose
     */
    public int readCommittedValue(Integer variableId) {

    }

    /**
     * update the committed value with its current value
     */
    public void commitVariable(int variableId) {

    }

    /**
     * write the current value
     */
    public void write(int variableId, int value) {

    }

    /**
     * read the current value
     */
    public int read(int variableId) {
        
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
            if(dataCopies.get(variable).isReadAvailable())
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
