import java.util.*;
public class LockManager {

    private Map<Integer, Map<Integer,LockType>> lockTable;
    private Map<Integer, List<Integer>> waitingList;

    /**
     * Initialize the lock manager
     */
    public LockManager(){
        lockTable = new HashMap<>();
        waitingList = new HashMap<>();
    }

    /**
     * Acquire a lock for a transaction and return failure information if acquisition failed
     * side effect: might change lockTable and waitingList
     * @param transactionId the transaction acquiring the lock
     * @param variableId the variable id
     * @param lockType the lock type (read / write)
     * @return if acquire lock successfully, return empty set; if blocked by waiting list, return a set contains - 1; if blocked by other lock holders, return a set containing conflicting transactions
     */
    public Set<Integer> acquireLock(int transactionId, int variableId, LockType lockType) {
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        List<Integer> waitingTransactions = waitingList.get(variableId);
        Set<Integer> conflictingTransactions = new HashSet<>();

        // if the waiting list is not empty, and this transaction is not at the top of the list, then should be blocked
        if (waitingTransactions != null) {
            if (waitingTransactions.size() > 0 && waitingTransactions.get(0) != transactionId) {
                addToWaitingList(transactionId, variableId);
                conflictingTransactions.add(-1);
                return conflictingTransactions;
            }
        }

        // if try to acquire read lock
        if (lockType == LockType.READ) {
            for (int transaction : locks.keySet()) {
                if (locks.get(transaction) == LockType.WRITE) {
                    // there could only be one transaction holding a write lock on this variable
                    addToWaitingList(transactionId, variableId);
                    conflictingTransactions.add(transactionId);
                    return conflictingTransactions;
                }
            }

            // acquire read lock successfully
            removeFromWaitingList(transactionId, variableId);
            addLock(lockType, transactionId, variableId);
            return new HashSet<>();
        }

        // if try to acquire write lock
        for (int transaction : locks.keySet()) {
            conflictingTransactions.add(transactionId);
        }
        // acquire write lock successfully
        if (conflictingTransactions.isEmpty()) {
            removeFromWaitingList(transactionId, variableId);
            addLock(lockType, transactionId, variableId);
            return new HashSet<>();
        }
        return conflictingTransactions;
    }

    /**
     * Release a single write lock, will only be called if the transaction failed to get write locks on all the available sites
     * side effect: will change lock table
     * @param transactionId the transaction id
     * @param variableId the variable id
     * @param holdingReadLock whether this transaction is having a read lock before it tries to obtain a write lock
     */
    public void releaseWriteLock(int transactionId, int variableId, boolean holdingReadLock) {
        // remove the lock
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        if (!locks.containsKey(transactionId)) {
            locks.remove(transactionId);
        }
        lockTable.put(variableId, locks);
        // if the transaction is holding read lock previously, need to add back the read lock
        if (holdingReadLock) {
            addLock(LockType.READ, transactionId, variableId);
        }
    }

    /**
     * Remove all the locks that this transaction has, will be called when commit or abort
     * side effect: might change lock table and waiting list
     * @param transactionId the transaction to commit or abort
     */
    public void releaseAllLocks(int transactionId) {
        // remove this transaction from lock table
        for (int variableId : lockTable.keySet()) {
            Map<Integer, LockType> locks = lockTable.get(variableId);
            locks.remove(transactionId);
        }
        // remove this transaction from waiting list
        for (int variableId : waitingList.keySet()) {
            removeFromWaitingList(transactionId, variableId);
        }

    }

    /**
     * Clear the lock table, will be called when site fails
     * side effect: will change lock table and waiting list
     */
    public void clear() {
        lockTable.clear();
        waitingList.clear();
    }

    /**
     * Helper method for adding a transaction to waiting list
     * side effect: will change waitingList
     * @param transactionId the transaction added to the waiting list
     * @param variableId which variable the transaction is waiting for
     */
    private void addToWaitingList(int transactionId, int variableId) {
        List<Integer> waitingTransactions = waitingList.getOrDefault(variableId, new ArrayList<>());
        waitingTransactions.add(transactionId);
        waitingList.put(variableId, waitingTransactions);
    }

    /**
     * Helper method for removing a transaction from waiting list
     * side effect: might change waitingList
     * @param transactionId the transaction that will be removed from the waiting list if it is on the waiting list
     * @param variableId which variable the transaction is waiting for
     */
    private void removeFromWaitingList(int transactionId, int variableId) {
        List<Integer> waitingTransactions = waitingList.getOrDefault(variableId, new ArrayList<>());
        waitingTransactions.remove(transactionId);
    }

    /**
     * Helper method for adding or upgrading a lock on the lock table
     * @param lockType the lock type (read / write)
     * @param transactionId the transaction acquiring the lock
     * @param variableId the variable id
     */
    private void addLock(LockType lockType, int transactionId, int variableId)
    {
        // obtain all the locks on this variable
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        // may add a new lock or upgrade an existing lock
        if (!locks.containsKey(transactionId) || lockType == LockType.WRITE) {
            locks.put(transactionId, lockType);
        }
        lockTable.put(variableId, locks);
    }
}