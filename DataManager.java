import com.sun.tools.internal.ws.wsdl.document.Output;
import javafx.util.Pair;

import javax.xml.crypto.Data;
import java.util.*;

public class DataManager {
    int siteId;
    Map<Integer, DataCopy> dataCopies;  // <key : variable id, value : data copy>

    /**
     * Initialize the data manager
     * @param siteId site id
     */
    public DataManager(int siteId) {
        this.siteId = siteId;
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
     * Dump the data information on this site
     * @param outputPrinter the printer object
     * @see OutputPrinter#dumpSite(int, Map)
     */
    public void dump(OutputPrinter outputPrinter) {
        outputPrinter.dumpSite(siteId, dataCopies);
    }

    /**
     * Check whether this variable is available for read on this site
     * @param variableId
     * @return true if is available for read, false if not available for read
     */
    public boolean readAvailable(int variableId) {
        DataCopy dCopy = dataCopies.get(variableId);
        if(dCopy.isReadAvailable()) return true;
        return false;
    }

    /**
     * Read the latest committed value of this variable, will be called by a transaction acquired read lock
     * @param variableId the variable id
     * @return the latest committed value
     */
    public int read(int variableId) {
        DataCopy dataCopy = dataCopies.get(variableId);
        return dataCopy.getLatestCommitValue();
    }

    /**
     * Get the latest commit information before a certain timestamp, will be called by read-only transaction
     * @param variableId the variable id
     * @param timestamp the beginning time of the read-only transaction
     * @return the latest commit time and the latest commit value before the timestamp
     */
    public Pair<Integer, Integer> getSnapshot(int variableId, int timestamp) {
        DataCopy dataCopy = dataCopies.get(variableId);
        List<Pair<Integer, Integer>> commitHistory = dataCopy.getCommitHistory();
        Pair<Integer, Integer> snapshot = commitHistory.get(0);
        for (int i = 1; i < commitHistory.size(); i++) {
            if (commitHistory.get(i).getKey() < timestamp) {
                snapshot = commitHistory.get(i);
            }
        }
        return snapshot;
    }

    /**
     * Commit a set of variables on relevant data copies held by this site
     * side effect: will change the readability and commit history of the data copies
     * @param time the time when this variable is committed
     * @param updatedVariables the updated values, <key : variable id, value : updated value>
     */
    public void commitVariables(int time, Map<Integer, Integer> updatedVariables) {
        for (int variableId : updatedVariables.keySet()) {
            DataCopy dataCopy = dataCopies.get(variableId);
            int value = updatedVariables.get(variableId);
            dataCopy.addCommitHistory(time, value);
            dataCopy.setReadAvailable(true);
        }
    }

    /**
     * Recover the readability of all the non-replicated data, will be called as soon as the site recovers
     * side effect: will change the readability of all the non-replicated data copies
     */
    public void setAllNonReplicatedDataAvailable() {
        for (int variableId : dataCopies.keySet()) {
            DataCopy dataCopy = dataCopies.get(variableId);
            if (dataCopy.getDataType() == DataType.NOT_REPLICATED) {
                dataCopy.setReadAvailable(true);
            }
        }
    }

    /**
     * Set all the data copy as not available for read, will be called when the site fails
     * side effect: will change the readability of all data copies
     */
    public void setAllDataUnavailable() {
        for (int variableId : dataCopies.keySet()) {
            DataCopy dataCopy = dataCopies.get(variableId);
            dataCopy.setReadAvailable(false);
        }
    }
}
