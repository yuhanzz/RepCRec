/**
 * records all the information that the transaction manager needs to know about a transaction
 */
public class Transaction{
    public int id;
    public int beginTime;
    private TransactionType type;
    public TransactionStatus status;
    private Set<Integer> accessedSites;
    private Map<Integer, LockType> holdingLocks;

    /**
     * return true if the current transaction is holding a required lock, or a higher rank lock
     */
    public boolean isHoldingLock(LockType lockType, int variableId) {
        if (!holdingLocks.containsKey(variableId)) {
            return false;
        }

        if (lockType == LockType.READ) {
            return true;
        }

        // if require write lock but only has read lock 
        if (holdingLocks.get(variableId) == LockType.READ) {
            return false;
        }

        return true;
    }

    public boolean isReadOnly() {
        return type == TransactionType.READ_ONLY;
    }
}