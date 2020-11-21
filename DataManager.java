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
    public int readCommittedValue() {

    }

    /**
     * update the committed value with its current value
     */
    public void commitVariable(int variableId) {

    }

    /**
     * write the current value
     */
    public void write(int transactionId, int variableId, int value) {

    }

    /**
     * read the current value
     */
    public void read() {
        
    }
    
}
