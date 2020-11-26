import java.util.Map;

public class OutputPrinter {
    private boolean verbose;
    public OutputPrinter(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     *
     * @param siteId
     * @param dataCopies
     * sample output: site 1 – x2: 6, x3: 2, ... x20: 3
     */
    public void dumpSite(int siteId, Map<Integer, DataCopy> dataCopies) {
        System.out.print("site " + siteId);
        boolean firstEntry = true;
        for (int i = 1; i <= 20; i++) {
            if (dataCopies.containsKey(i)) {
                if (firstEntry) {
                    System.out.print(" – ");
                    firstEntry = false;
                } else {
                    System.out.print(", ");
                }
                System.out.print("x" + i + ": " + dataCopies.get(i).getCommittedValue());
            }
        }
        System.out.print('\n');
    }

    public void printReadSuccess(int variableId, int value, int transactionId) {
        System.out.print("x" + variableId + ": " + value);
        if (verbose) {
            System.out.print(" read by T" + transactionId);
        }
        System.out.print('\n');
    }

    public void printWriteSuccess(int variableId, int value, int transactionId) {
        if (verbose) {
            System.out.print(value + " written to x" + variableId + " by T" + transactionId + '\n');
        }
    }

    public void printCommitSuccess(int transactionId) {
        System.out.println("T" + transactionId + " commit");
    }

    public void printAbortSuccess(int transactionId) {
        System.out.println("T" + transactionId + " abort");
    }

    public void printDeadlock(int transactionId) {
        if (verbose) {
            System.out.println("Choose T" + transactionId + "to abort for dead lock");
        }
    }
}
