package src.main.java;

import java.util.Map;
import java.util.Set;

public class OutputPrinter {

    private boolean verbose;
    StringBuffer buffer = new StringBuffer();

    /**
     * Constructor for OutputPrinter
     * @param verbose if -v is in the arguments in the main 
     * @author Lillian Huang 
     */
    public OutputPrinter(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Prints the output 
     * @author Lillian Huang 
     */
    public void print() {
        System.out.println(buffer.toString());
    }

    /**
     * Prints out all the data copies values in each site 
     * @param siteId
     * @param dataCopies
     * sample output: site 1 – x2: 6, x3: 2, ... x20: 3
     * @author Lillian Huang 
     */
    public void dumpSite(int siteId, Map<Integer, DataCopy> dataCopies) {
        buffer.append("site " + siteId);
        boolean firstEntry = true;
        for (int i = 1; i <= 20; i++) {
            if (dataCopies.containsKey(i)) {
                if (firstEntry) {
                    buffer.append(" – ");
                    firstEntry = false;
                } else {
                    buffer.append(", ");
                }
                buffer.append("x" + i + ": " + dataCopies.get(i).getLatestCommitValue());
            }
        }
        buffer.append('\n');
    }

    /**
     * Prints out the read variable value by which transaction 
     * @param variableId which variable is being read 
     * @param value the value of the variable 
     * @param transactionId which transaction read the variable 
     * @author Lillian Huang 
     */
    public void printReadSuccess(int variableId, int value, int transactionId) {
        buffer.append("x" + variableId + ": " + value);
        if (verbose) {
            buffer.append(" read by T" + transactionId);
        }
        buffer.append('\n');
    }

    /**
     * Prints out the variable being written to by which transaction 
     * @param variableId which variable is being written to 
     * @param value the value that is being written in 
     * @param transactionId which transaction is writing to the variable 
     * @author Lillian Huang 
     */
    public void printWriteSuccess(int variableId, int value, int transactionId) {
        if (verbose) {
            buffer.append(value + " written to x" + variableId + " by T" + transactionId + '\n');
        }
    }

    /**
     * Prints out the transaction that has been successfully commited 
     * @param transactionId the transaction that has been successfully commited 
     * @author Lillian Huang 
     */
    public void printCommitSuccess(int transactionId) {
        buffer.append("T" + transactionId + " commit\n");
    }

    /**
     * Prints out the transaction that has been aborted 
     * @param transactionId the transaction that has been aborted 
     * @author Lillian Huang 
     */
    public void printAbortSuccess(int transactionId) {
        buffer.append("T" + transactionId + " abort\n");
    }

    /**
     * Prints out the transaction that will be aborted due to deadlock 
     * @param transactionId the transaction that will be aborted due to deadlock 
     * @author Lillian Huang 
     */
    public void printDeadlock(int transactionId) {
        if (verbose) {
            buffer.append("Choose the youngest transaction T" + transactionId + " to abort\n");
        }
    }

    /**
     * Prints out the waitsfor graph 
     * @param waitsForGraph the current waitsfor graph 
     * @author Lillian Huang 
     */
    public void printWaitsForGraph(Map<Integer, Set<Integer>> waitsForGraph) {
        if (verbose) {
            buffer.append("waits for graph:\n");
            for (int sourceNode : waitsForGraph.keySet()) {
                for (int destNode : waitsForGraph.get(sourceNode)) {
                    buffer.append("T" + sourceNode + " -> T" + destNode + '\n');
                }
            }
        }
    }

    /**
     * Prints out the cycle if there is one 
     * @param cycle the cycle causing the deadlock 
     * @author Lillian Huang 
     */
    public void printCycle(Set<Integer> cycle) {
        if (verbose && !cycle.isEmpty()) {
            buffer.append("cycle detected:");
            for (int node : cycle) {
                buffer.append(" T" + node);
            }
            buffer.append('\n');
        }
    }
}
