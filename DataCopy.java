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


    public DataCopy(DataType dataType, int initialValue)
    {
        this.commitHistory = new ArrayList<>();
        commitHistory.add(new Pair<>(-1, initialValue));
        this.readAvailable = true;
        this.dataType = dataType;
    }

    public boolean isReadAvailable() {
        return readAvailable;
    }

    public void setReadAvailable(boolean readAvailable) {
        this.readAvailable = readAvailable;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void addCommitHistory(int time, int value) {
        commitHistory.add(new Pair<>(time, value));
    }

    public int getLatestCommitValue() {
        Pair<Integer, Integer> latestCommit = commitHistory.get(commitHistory.size() - 1);
        return latestCommit.getValue();
    }

    public List<Pair<Integer, Integer>> getCommitHistory() {
        return commitHistory;
    }
}