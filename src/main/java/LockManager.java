package src.main.java;

import javafx.util.Pair;
import java.util.*;

public class LockManager {

    private Map<Integer, Map<Integer,LockType>> lockTable;

    /**
     * Initialize the lock manager
     * @author Lillian Huang 
     */
    public LockManager(){
        lockTable = new HashMap<>();

    }

    /**
     * Acquire a lock for a transaction and return failure information if acquisition failed
     * side effect: might change lockTable
     * @param transactionId the transaction acquiring the lock
     * @param variableId the variable id
     * @param lockType the lock type (read / write)
     * @return if acquire lock successfully, return empty set; if blocked, return a set containing conflicting transactions
     * @author Lillian Huang 
     */
    public Set<Integer> acquireLock(int transactionId, int variableId, LockType lockType) {
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        Set<Integer> conflictingTransactions = new HashSet<>();

        // if acquiring read lock
        if (lockType == LockType.READ) {
            for (int transaction : locks.keySet()) {
                if (locks.get(transaction) == LockType.WRITE) {
                    conflictingTransactions.add(transaction);
                    return conflictingTransactions;
                }
            }

            // acquire read lock successfully
            addLock(lockType, transactionId, variableId);
            return new HashSet<>();
        }

        // if acquire write lock successfully
        if (locks.keySet().isEmpty() || (locks.keySet().size() == 1 && locks.containsKey(transactionId))) {
            addLock(lockType, transactionId, variableId);
            return new HashSet<>();
        }

        // acquire write lock failed
        for (int transaction : locks.keySet()) {
            conflictingTransactions.add(transaction);
        }
        return conflictingTransactions;
    }

    /**
     * Release a single write lock, will only be called if the transaction failed to get write locks on all the available sites
     * side effect: will change lock table
     * @param transactionId the transaction id
     * @param variableId the variable id
     * @param holdingReadLock whether this transaction is having a read lock before it tries to obtain a write lock
     * @author Lillian Huang 
     */
    public void releaseWriteLock(int transactionId, int variableId, boolean holdingReadLock) {
        // remove the lock
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        if (locks.containsKey(transactionId)) {
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
     * side effect: might change lock table
     * @param transactionId the transaction to commit or abort
     * @author Lillian Huang 
     */
    public void releaseAllLocks(int transactionId) {
        // remove this transaction from lock table
        for (int variableId : lockTable.keySet()) {
            Map<Integer, LockType> locks = lockTable.get(variableId);
            locks.remove(transactionId);
        }

    }

    /**
     * Clear the lock table, will be called when site fails
     * side effect: will change lock table
     * @author Lillian Huang 
     */
    public void clear() {
        lockTable.clear();
    }


    /**
     * Helper method for adding or upgrading a lock on the lock table
     * @param lockType the lock type (read / write)
     * @param transactionId the transaction acquiring the lock
     * @param variableId the variable id
     * @author Lillian Huang 
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

    /**
     * Check whether the transaction is holding the lock
     * @param lockType the lock type
     * @param variableId the variable id
     * @param transactionId the transaction id
     * @return true if the transaction is holding the lock, false if not
     * @author Lillian Huang 
     */
    public boolean isHoldingLock(LockType lockType, int variableId, int transactionId) {
        if (!lockTable.containsKey(variableId)) {
            return false;
        }
        Map<Integer,LockType> locks = lockTable.get(variableId);

        if (!locks.containsKey(transactionId)) {
            return false;
        }

        if (locks.get(transactionId) == LockType.READ && lockType == LockType.WRITE) {
            return false;
        }
        return true;
    }
}