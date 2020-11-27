import javafx.util.Pair;
import java.util.*;

public class LockManager {

    private Map<Integer, Map<Integer,LockType>> lockTable;
    private Map<Integer, List<Pair<Integer, LockType>>> waitingList;

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
     * @return if acquire lock successfully, return empty set; if blocked, return a set containing conflicting transactions
     */
    public Set<Integer> acquireLock(int transactionId, int variableId, LockType lockType) {
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        List<Pair<Integer, LockType>> waitlist = waitingList.getOrDefault(variableId, new ArrayList<>());

        // if is not at the top of wait list, add to wait list
        if (!waitlist.isEmpty() && waitlist.get(0).getKey() != transactionId) {
            Set<Integer> conflictingTransactions = getConflictingTransaciton(transactionId, variableId, lockType);
            addToWaitingList(transactionId, variableId, lockType);
            return conflictingTransactions;
        }

        // check whether this is retry
        boolean retry = false;
        if (!waitlist.isEmpty()) {
            retry = true;
        }

        // if acquiring read lock
        if (lockType == LockType.READ) {
            for (int transaction : locks.keySet()) {
                if (locks.get(transaction) == LockType.WRITE) {
                    if (retry) {
                        Set<Integer> conflictingTransactions = new HashSet<>();
                        conflictingTransactions.add(transaction);
                        return conflictingTransactions;
                    }
                    Set<Integer> conflictingTransactions = getConflictingTransaciton(transactionId, variableId, lockType);
                    addToWaitingList(transactionId, variableId, lockType);
                    return conflictingTransactions;
                }
            }

            // acquire read lock successfully
            if (retry) {
                popWaitingList(variableId);
            }
            addLock(lockType, transactionId, variableId);
            return new HashSet<>();
        }

        // if try to acquire write lock
        if (!locks.keySet().isEmpty()) {
            if (retry) {
                return locks.keySet();
            }
            Set<Integer> conflictingTransactions = getConflictingTransaciton(transactionId, variableId, lockType);
            addToWaitingList(transactionId, variableId, lockType);
            return conflictingTransactions;
        }

        // acquire write lock successfully
        if (retry) {
            popWaitingList(variableId);
        }
        addLock(lockType, transactionId, variableId);
        return new HashSet<>();
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
            List<Pair<Integer, LockType>> waitlist = waitingList.getOrDefault(variableId, new ArrayList<>());
            waitlist.removeIf(item -> (item.getKey() == transactionId));
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
     * Find the transactions to wait for
     * @param transactionId the transaction id
     * @param variableId the variable id
     * @param locktype the lock type that attempt to acquire
     * @return the set of transactions that the current transaction needs to wait for
     */
    private Set<Integer> getConflictingTransaciton(int transactionId, int variableId, LockType locktype) {
        List<Pair<Integer, LockType>> list = waitingList.get(variableId);

        // prepend the transactions that currently holding the lock to the list
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        for (int transaction : locks.keySet()) {
            Pair<Integer, LockType> lock = new Pair<>(transaction, locks.get(transaction));
            list.add(0, lock);
        }

        // find the most recent transactions that have conflict
        Set<Integer> conflictingTransactions = new HashSet<>();

        // if acquiring read lock, find the latest write lock
        if (locktype == LockType.READ) {
            for (int i = list.size() - 1; i >= 0; i--) {
                int currentTransaction = list.get(i).getKey();
                LockType currentLockType = list.get(i).getValue();
                if (currentLockType == LockType.WRITE) {
                    if (currentTransaction != transactionId) {
                        conflictingTransactions.add(currentTransaction);
                    }
                    break;
                }
            }
            return conflictingTransactions;
        }

        // if acquiring write lock, find the latest read locks or a latest write lock that blocks this write lock request

        // if there is no blocking transactions
        if (list.isEmpty()) {
            return conflictingTransactions;
        }

        LockType blockingType = list.get(list.size() - 1).getValue();
        // if the write lock is blocked by a single write lock
        if (blockingType == LockType.WRITE) {
            conflictingTransactions.add(list.get(list.size() - 1).getKey());
            return conflictingTransactions;
        }
        // if the write lock is blocked by a set of read locks
        for (int i = list.size() - 1; i >= 0; i--) {
            int currentTransaction = list.get(i).getKey();
            LockType currentLockType = list.get(i).getValue();
            if (currentLockType == LockType.WRITE) {
                break;
            }
            if (currentTransaction != transactionId) {
                conflictingTransactions.add(currentTransaction);
            }
        }
        return conflictingTransactions;
    }

    /**
     * Helper method for adding a transaction to waiting list
     * side effect: will change waitingList
     * @param transactionId the transaction id
     * @param variableId the variable id
     * @param lockType the lock type
     */
    private void addToWaitingList(int transactionId, int variableId, LockType lockType) {
        List<Pair<Integer, LockType>> waitlist = waitingList.getOrDefault(variableId, new ArrayList<>());
        waitlist.add(new Pair<>(transactionId, lockType));
        waitingList.put(variableId, waitlist);
    }

    /**
     * Helper method for removing the first waiting transaction from waiting list
     * side effect: might change waitingList
     * @param variableId the variable id
     */
    private void popWaitingList(int variableId) {
        if (waitingList.containsKey(variableId)) {
            List<Pair<Integer, LockType>> waitlist = waitingList.get(variableId);
            if (!waitlist.isEmpty()) {
                waitlist.remove(0);
            }
        }
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