import java.util.*;
class Site {
    int siteId;
    DataManager dataManager;
    LockManager lockManager;
    SiteStatus siteStatus;
    int latestFailedTime;

    /**
     * initialize the Site
     */
    public Site(int siteId) {
        this.siteId = siteId;
        this.dataManager = new DataManager(siteId);
        this.lockManager = new LockManager();
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
     * return the set of conflicting transactions if can not
     */
    public Set<Integer> lockAvailable(LockType lockType, int variableId) {
        if(lockType == LockType.READ)
        {
            return lockManager.readLockAvailable(variableId);
        }
        return lockManager.writeLockAvailable(variableId);
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
        Set<Integer> writtenVariables = lockManager.getAllWrittenVariables(transactionId);
        for(Integer variableId: writtenVariables)
        {
            // update the current value to committed value
            dataManager.commitVariable(variableId);
            // if the readAvailable of this data copy is false, change it to true
            dataManager.updateReadAvail(variableId, true);
        }
        // release all the locks
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