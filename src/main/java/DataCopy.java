package src.main.java;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * information about the data copy stored on the data manager
 */

class DataCopy {
    private List<Pair<Integer, Integer>> commitHistory;
    private boolean readAvailable;
    private DataType dataType;

    /**
     * Constructor that initalizes the commitHistory, sets dataType, inital value of the variable 
     * and also readavailable to be true 
     * @param dataType
     * @param initialValue
     * @author Lillian Huang 
     */
    public DataCopy(DataType dataType, int initialValue)
    {
        this.commitHistory = new ArrayList<>();
        commitHistory.add(new Pair<>(-1, initialValue));
        this.readAvailable = true;
        this.dataType = dataType;
    }

    /**
     * Getter of readAvailable
     * @return true if able to read this DataCopy else false 
     * @author Lillian Huang
     */
    public boolean isReadAvailable() {
        return readAvailable;
    }

    /**
     * Setter of readAvailable
     * @param readAvailable the value being set for readAvailable
     * @author Lillian Huang
     */
    public void setReadAvailable(boolean readAvailable) {
        this.readAvailable = readAvailable;
    }

    /**
     * getter of DataType
     * @return the DataType of this DataCopy 
     * @author Lillian Huang
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * Adding committed value into the commitHistory
     * @param time the time of this commited value
     * @param value the value of the DataCopy at this time 
     * @author Lillian Huang
     */
    public void addCommitHistory(int time, int value) {
        commitHistory.add(new Pair<>(time, value));
    }

    /**
     * Getting the latest committed value of the DataCopy
     * @return the latest committed value of the DataCopy
     * @author Lillian Huang
     */
    public int getLatestCommitValue() {
        Pair<Integer, Integer> latestCommit = commitHistory.get(commitHistory.size() - 1);
        return latestCommit.getValue();
    }

    /**
     * Getting the whole CommitHistory of this DataCopy
     * @return the whole CommitHistory of this DataCopy
     * @author Lillian Huang
     */
    public List<Pair<Integer, Integer>> getCommitHistory() {
        return commitHistory;
    }
}