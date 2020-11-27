package src.main.java;

import java.util.*;
class Site {
    int siteId;
    OutputPrinter outputPrinter;
    DataManager dataManager;
    LockManager lockManager;
    SiteStatus siteStatus;

    /**
     * initialize the src.main.java.Site
     */
    public Site(int siteId, OutputPrinter outputPrinter) {
        this.siteId = siteId;
        this.outputPrinter = outputPrinter;
        this.dataManager = new DataManager(siteId);
        this.lockManager = new LockManager();
        this.siteStatus = siteStatus.UP;
    }

    /**
     * Dump the site
     */
    public void dump() {
        dataManager.dump(outputPrinter);
    }


    /**
     * Check whether the site is up
     * @return true if the site is up, false if the site is down
     */
    public boolean isUp() {
        return siteStatus == SiteStatus.UP;
    }

    /**
     * Get the data manager
     * @return data manager
     */
    public DataManager getDataManager() {
        return dataManager;
    }

    /**
     * Get the lock manager
     * @return lock manager
     */
    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Call data manager and lock manager to commit this transaction
     * side effect: will change data manager and lock manager
     * @param transactionId the transaction to commit
     * @param updatedVariables the updated values of the variables touched by this transaction
     */
    public void commit(int transactionId, int time, Map<Integer, Integer> updatedVariables) {
        dataManager.commitVariables(time, updatedVariables);
        lockManager.releaseAllLocks(transactionId);
    }

    /**
     * Call lock manager to abort this transaction
     * side effect: will change lock manager
     * @param transactionId the transaction to abort
     */
    public void abort(int transactionId) {
        lockManager.releaseAllLocks(transactionId);
    }

    /**
     * Simulate site failure
     * side effect: will change siteStatus, lock manager, data manager
     */
    public void fail() {

        siteStatus = SiteStatus.DOWN;
        lockManager.clear();
        dataManager.setAllDataUnavailable();
    }

    /**
     * simulate site recovery
     * side effect: will change siteStatus, data manager
     */
    public void recover() {

        siteStatus = SiteStatus.UP;
        dataManager.setAllNonReplicatedDataAvailable();
    }
}