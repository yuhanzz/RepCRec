package src.main.java;

import java.util.Map;
import java.util.Set;

public class OutputPrinter {
    private boolean verbose;
    StringBuffer buffer = new StringBuffer();
    public OutputPrinter(boolean verbose) {
        this.verbose = verbose;
    }

    public void print() {
        System.out.println(buffer.toString());
    }

    /**
     *
     * @param siteId
     * @param dataCopies
     * sample output: site 1 – x2: 6, x3: 2, ... x20: 3
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

    public void printReadSuccess(int variableId, int value, int transactionId) {
        buffer.append("x" + variableId + ": " + value);
        if (verbose) {
            buffer.append(" read by T" + transactionId);
        }
        buffer.append('\n');
    }

    public void printWriteSuccess(int variableId, int value, int transactionId) {
        if (verbose) {
            buffer.append(value + " written to x" + variableId + " by T" + transactionId + '\n');
        }
    }

    public void printCommitSuccess(int transactionId) {
        buffer.append("T" + transactionId + " commit\n");
    }

    public void printAbortSuccess(int transactionId) {
        buffer.append("T" + transactionId + " abort\n");
    }

    public void printDeadlock(int transactionId) {
        if (verbose) {
            buffer.append("Choose the youngest transaction T" + transactionId + " to abort\n");
        }
    }

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
