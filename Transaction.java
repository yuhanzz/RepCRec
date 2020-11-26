import java.util.*;
/**
 * records all the information that the transaction manager needs to know about a transaction
 */
public class Transaction{
    public int id;
    public int beginTime;
    private TransactionType type;
    public TransactionStatus status;
    private Map<Integer, Integer> accessedSites; // <key : accessed site id, value : firstAccessedTiem>
    private Map<Integer, LockType> holdingLocks;    // <key : variable id, value : the lock level held on this variable (Read / Write)>
    private Map<Integer, Integer> localCache;   // <key : variable id, value : current value>


    public Transaction(int id, int beginTime, TransactionType type) {
        this.id = id;
        this.beginTime = beginTime;
        this.type = type;
        this.status = TransactionStatus.ACTIVE;
        this.accessedSites = new HashMap<>();
        this.holdingLocks = new HashMap<>();
    }

    public TransactionType getType() {
        return type;
    }

    public int getBeginTime() {
        return beginTime;
    }

    /**
     * 
     */

    public void addAccessedSite(int firstAccessTime, int siteId) {
        if (accessedSites.containsKey(siteId)) {
            return;
        }
        accessedSites.put(siteId, firstAccessTime);
    }

    public Map<Integer, Integer> getAccessedSites() {
        return accessedSites;
    }

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

    public void addLock(LockType lockType, int variableId) {
        if (isHoldingLock(lockType, variableId)) {
            return;
        }
        holdingLocks.put(variableId, lockType);
    }

    public int read(int variableId) {
        return localCache.get(variableId);
    }

    public void write(int variableId, int value) {
        localCache.put(variableId, value);
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void cache(int variableId, int value) {
        localCache.put(variableId, value);
    }

    public Map<Integer, Integer> getLocalCache() {
        return localCache;
    }
}