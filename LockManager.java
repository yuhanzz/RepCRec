public class LockManager {

    /**
     * return the set of variables that this transaction has write lock on
     */
    Set<Integer> getAllWrittenVariables(int transactionId) {

    }

    /**
     * release all the locks that this transaction has
     */
    void releaseAllLocks(int transactionId) {

    }

    /**
     * clear the whole lock table due to site failure
     */
    void clear() {

    }
}