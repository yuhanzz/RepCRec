/**
 * records all the information that the transaction manager needs to know about a variable
 */

public final class DataInfo {

    private final DataType type;
    private final List<Integer> availableSites;

    public DataInfo(int variableId, DataType type, List<Integer> availableSites) {
        this.variableId = variableId;
        this.type = type;
        this.availableSites = availableSites;
    }

    public void getAvailableSites() {
        return availableSites;
    }
}
