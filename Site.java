import java.util.*;
class Site {
    DataManager dataManager;
    LockManager lockManager;
    SiteStatus siteStatus;
    int latestFailedTime;

    /**
     * initialize the Site
     */
    public Site() {
        // need to figure this out for dataManager and lockManager
        this.siteStatus = siteStatus.UP;
        this.latestFailedTime = -1;
    }
    
    public SiteStatus getSiteStatus() {
        return this.siteStatus;
    }

    /**
     * return true if the readAvailable of this data copy is true
     */
    public boolean readAvailable(int variableId) {
        if(dataManager.readAvailable(variableId)) return true;
        return false;
    }

    /**
     * return the empty set if can acquire this lock
     * return the set of conflicting transactions if failed
     */
    public Set<Integer> lockAvailable(LockType lockType, int variableId) {
        Set<Integer> conflictingTransactions = new HashSet<>(); 
        if(lockType == LockType.READ)
        {
            if(readAvailable(variableId))
            {
                return conflictingTransactions;
            }
        }
        else
        {
            if(lockManager.getAllTrasactionIDS(variableId).isEmpty())
            {
                return conflictingTransactions;
            }
        }
        return lockManager.getAllTrasactionIDS(variableId);
    }

    /**
     * change the lock table accordingly
     */
    public void acquireLock(LockType lockType, int transactionId, int variableId) {
        lockManager.addLock(lockType, transactionId, variableId);
    }

    /**
     * return the current value of the data copy
     */
    public int read(int variableId) {
        return dataManager.read(variableId); 
    }

    /**
     * change the current value of the data copy
     */
    public void write(int variableId, int value) {
        dataManager.write(variableId, value);
    }

    /**
     * return true if Site is UP and firstAccessTime is larger than latestFailedTime
     */
    public boolean commitReady(int firstAccessTime) {
        return siteStatus == SiteStatus.UP && firstAccessTime > latestFailedTime;
    }

    /**
     * commit this transaction on this site
     */
    public void commit(int transactionId) {

        // update the current value to committed value (only update the variables that this transaction has write lock on)
        // if the readAvailable of this data copy is false, change it to true (only update the variables that this transaction has write lock on)
        // release all the locks
        Set<Integer> writeLocks = lockManager.getAllWrittenVariables(transactionId);
        for(Integer variableId: writeLocks)
        {
            dataManager.commitVariable(variableId);
            if(dataManager.readAvailable(variableId))
            {
                dataManager.updateReadAvail(variableId, true);
            }
        }
        lockManager.releaseAllLocks(transactionId);
    }

    /**
     * abort this transaction on this site if the site is UP, do nothing is the site is DOWN
     */
    public void abort(int transactionId) {
        lockManager.releaseAllLocks(transactionId);
        // release all the locks
    }

    public Map<Integer, Integer> takeSnapShot() {
        // if the site is down, return empty set
        // for all the data items in this site whose readAvailable is true, we send its committed value
        Map<Integer, Integer> map = new HashMap<>();
        if(siteStatus == SiteStatus.DOWN)
        {
            return map;
        }
        else
        {
            Set<Integer> items = dataManager.getAllReadAvail();
            for(Integer item: items)
            {
                map.put(item,dataManager.readCommittedValue(item));
            }
        }
        return map;
        
    }

    /**
     * return the curren time after site fail event
     */
    public int fail(int time) {

        // change the site status
        siteStatus = SiteStatus.DOWN;
        // remove all the locks on this site
        lockManager.clear();
        // set the latestFailedTime to current time
        latestFailedTime = time;
        // set the readAvailable of all the data copies as false
        Set<Integer> set = dataManager.getAllReadAvail();
        for(Integer item: set)
        {
            dataManager.updateReadAvail(item, false);
        }

        return time + 1;
    }
    
    /**
     * return the time after recover finishes
     */
    public int recover(int time) {

        // change the site status
        siteStatus = SiteStatus.UP;
        // for all the non-replicated data copies on this site, set its readAvailable to true
        dataManager.setAllNonReplicatedReadAvail(true);

        return time + 1;
    }
}