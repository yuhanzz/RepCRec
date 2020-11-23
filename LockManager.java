import java.util.*;
import javafx.util.Pair;
public class LockManager {

    private Map<Integer, Map<Integer,LockType>> lockTable;

    public LockManager(){
        this.lockTable = new HashMap<>();
    }

    /**
     * return empty set if can acquire read lock on this variable
     * return the set of conflicting transactions if can not acquire read lock on this variable
     */
    public Set<Integer> readLockAvailable(int variableId) {
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());

        for (int transactionId : locks.keySet()) {
            if (locks.get(transactionId) == LockType.WRITE) {
                // there could only be one transaction holding a write lock on this variable
                Set<Integer> conflictingTransactions = new HashSet<>();
                conflictingTransactions.add(transactionId);
                return conflictingTransactions;
            }
        }

        return new HashSet<>();
    }

    /**
     * return empty set if can acquire write lock on this variable
     * return the set of conflicting transactions if can not acquire write lock on this variable
     */

    public Set<Integer> writeLockAvailable(int variableId) {
        Map<Integer, LockType> locks = lockTable.getOrDefault(variableId, new HashMap<>());
        Set<Integer> conflictingTransactions = new HashSet<>();

        for (int transactionId : locks.keySet()) {
            conflictingTransactions.add(transactionId);
        }
        return conflictingTransactions;
    }

    /**
     * upgrade an existing lock or add a new lock
     */
    public void addLock(LockType lockType, int transactionId, int variableId)
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
     * return the set of variables that this transaction has write lock on
     */
    public Set<Integer> getAllWrittenVariables(int transactionId) {
        Set<Integer> set = new HashSet<>(); 
        Set<Integer> variableIds= lockTable.keySet(); 
        for(Integer variableId: variableIds)
        {
            Map<Integer, LockType> locks = lockTable.get(variableId);
            if (locks.get(transactionId) == LockType.WRITE) {
                set.add(variableId);
            }
        }
        return set;
    }

    /**
     * release all the locks that this transaction has
     */
    void releaseAllLocks(int transactionId) {
        for (int variableId : lockTable.keySet()) {
            Map<Integer, LockType> locks = lockTable.get(variableId);
            locks.remove(transactionId);
        }
    }

    /**
     * clear the whole lock table due to site failure
     */
    public void clear() {
        lockTable.clear();
    }
}