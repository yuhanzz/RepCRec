package src.main.java;
/**
 * records all the information that the transaction manager needs to know about a variable
 */
import java.util.*;
public final class DataInfo {

    private final DataType type; //Replicated or NonReplicated Data
    private final int variableId;
    private final List<Integer> availableSites; // The sites that have the copy of this variable

    /**
     * Constructor initializing all the information for the specific variable
     * @param variableId The variable 
     * @param type The DataType (Replicated or NonReplicated Data)
     * @param availableSites The sites that have the copy of this variable
     */
    public DataInfo(int variableId, DataType type, List<Integer> availableSites) {
        this.variableId = variableId;
        this.type = type;
        this.availableSites = availableSites;
    }

    /**
     * Getter of all the sites that has this variable 
     * @return all the sites that have the copy of this variable 
     */
    public List<Integer> getAvailableSites() {
        return availableSites;
    }
}
