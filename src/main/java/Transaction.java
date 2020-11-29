package src.main.java;

import java.util.*;
/**
 * records all the information that the transaction manager needs to know about a transaction
 */
public class Transaction{
    public int id;
    public int beginTime;
    private TransactionType type;
    public TransactionStatus status;
    private Map<Integer, Integer> accessedSites; // <key : accessed site id, value : firstAccessedTime>
    private Map<Integer, LockType> holdingLocks;    // <key : variable id, value : the lock level held on this variable (Read / Write)>
    private Map<Integer, Integer> localCache;   // <key : variable id, value : current value>


    /**
     * Constructor for Transaction 
     * @param id transaction id
     * @param beginTime begin time of the transaction 
     * @param type Type of Transaction 
     * @author Yuhan Zhou
     */
    public Transaction(int id, int beginTime, TransactionType type) {
        this.id = id;
        this.beginTime = beginTime;
        this.type = type;
        this.status = TransactionStatus.ACTIVE;
        this.accessedSites = new HashMap<>();
        this.holdingLocks = new HashMap<>();
        this.localCache = new HashMap<>();
    }

    /**
     * Getter of Type of Transaction 
     * @return Type of Transaction 
     * @author Yuhan Zhou 
     */
    public TransactionType getType() {
        return type;
    }

    /**
     * begin time of Transaction 
     * @return time of Transaction  
     * @author Yuhan Zhou 
     */
    public int getBeginTime() {
        return beginTime;
    }

    /**
     * Adds the site that is accessed and the time it was accessed only in the accessedSites map 
     * if it has not been recorded before 
     * @param firstAccessTime the time this site has been accessed 
     * @param siteId the site that has been accessed 
     * @author Yuhan Zhou 
     */

    public void addAccessedSite(int firstAccessTime, int siteId) {
        if (accessedSites.containsKey(siteId)) {
            return;
        }
        accessedSites.put(siteId, firstAccessTime);
    }

    /**
     * Getter of accessed sites of this transaction 
     * @return the sites that were accessed by this transaction 
     * @author Yuhan Zhou 
     */
    public Map<Integer, Integer> getAccessedSites() {
        return accessedSites;
    }

    /**
     * check if the current transaction is holding a required lock, or a higher rank lock
     * @param lockType the type of lock that we want to check if the transaction is holding
     *  this lock or a higher lock on the specified variableId
     * @param variableId the variable we are checking 
     * @return true if the current transaction is holding a required lock, or a higher rank lock
     * @author Yuhan Zhou 
     */
    public boolean isHoldingLock(LockType lockType, int variableId) {
        if (!holdingLocks.containsKey(variableId)) {
                return false;
        }
    
        if (lockType == holdingLocks.get(variableId))
        {
            return true; 
        }
        // in this case locktype must be different from the locktype in the holdinglocks map 
        // so if the lockType is a read lock, the locktype in the holdinglock map on this variable should be a write lock 
        // since write lock is a higher rank lock, it automatically also has the read lock. 
        return lockType == LockType.READ;
    }

    /**
     * checking if the transaction is a READ_ONLY transaction 
     * @return true if the current transaction is READ_ONLY
     * @author Yuhan Zhou 
     */
    public boolean isReadOnly() {
        return type == TransactionType.READ_ONLY;
    }

    /**
     * Adding a lock based on lockType on the specified variable 
     * (only if it is not holding lock already or not holding the high-rank lock)
     * @param lockType lockType that we want to add on the variable
     * @param variableId the variable we want to add the lock on 
     */
    public void addLock(LockType lockType, int variableId) {
        if (isHoldingLock(lockType, variableId)) {
            return;
        }
        holdingLocks.put(variableId, lockType);
    }

    /**
     * Getting the current value of the variable
     * @param variableId the variable we want the current value of 
     * @return the current value of the variable 
     * @author Yuhan Zhou 
     */
    public int read(int variableId) {
        return localCache.get(variableId);
    }

    /**
     * Writing the current value of the variable 
     * @param variableId the variable we want to update the value of 
     * @param value the updated value of the variable 
     * @author Yuhan Zhou 
     */
    public void write(int variableId, int value) {
        localCache.put(variableId, value);
    }

    /**
     * Setting the status of the transaction 
     * @param status the status that we are going to set the current status of the transaction to 
     * @author Yuhan Zhou 
     */
    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    /**
     * Getting the current status of the transaction 
     * @return the current status of the transaction 
     * @author Yuhan Zhou 
     */
    public TransactionStatus getStatus() {
        return status;
    }

    /**
     * Adding the variable and the corresponding value into the local cache 
     * @param variableId the variable we are putting in the local cache
     * @param value the value of the variable 
     * @author Yuhan Zhou 
     */
    public void cache(int variableId, int value) {
        localCache.put(variableId, value);
    }

    /**
     * Getter of the localCache 
     * @return the localCache of this transaction 
     * @author Yuhan Zhou 
     */
    public Map<Integer, Integer> getLocalCache() {
        return localCache;
    }
}