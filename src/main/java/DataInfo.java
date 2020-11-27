package src.main.java;
/**
 * records all the information that the transaction manager needs to know about a variable
 */
import java.util.*;
public final class DataInfo {

    private final DataType type;
    private final int variableId;
    private final List<Integer> availableSites;

    public DataInfo(int variableId, DataType type, List<Integer> availableSites) {
        this.variableId = variableId;
        this.type = type;
        this.availableSites = availableSites;
    }

    public List<Integer> getAvailableSites() {
        return availableSites;
    }
}
