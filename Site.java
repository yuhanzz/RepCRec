class Site {
    DataManager dataManager;
    LockManager lockManager;
    SiteStatus siteStatus;
    int latestFailedTime;

    /**
     * initialize the Site
     */
    public Site() {

    }
    
    public SiteStatus getSiteStatus() {

    }

    /**
     * return true if the readAvailable of this data copy is true
     */
    public boolean readAvailable(int variableId) {

    }

    /**
     * return the empty set if acquiring succeeds
     * return the set of conflicting transactions if failed
     */
    public Set<Integer> lockAvailable(LockType lockType, int transactionId, int variableId) {

    }

    /**
     * change the lock table accordingly
     */
    public void acquireLock(LockType lockType, int transactionId, int variableId) {

    }

    /**
     * return the curren value of the data copy
     */
    public int read(int variableId) {

    }

    /**
     * change the current value of the data copy
     */
    public void write() {

    }

    /**
     * return true if Site is UP and firstAccessTime is larger than latestFailedTime
     */
    public boolean commitReady(int firstAccessTime) {

    }

    /**
     * commit this transaction on this site
     */
    public void commit(int transactionId) {

        // update the current value to committed value (only update the variables that this transaction has write lock on)

        // if the readAvailable of this data copy is false, change it to true (only update the variables that this transaction has write lock on)

        // release all the locks
    }

    /**
     * abort this transaction on this site
     */
    public void abort(int transactionId) {
        
        // release all the locks
    }

    public Map<Integer, Integer> takeSnapShot() {
        // if the site is down, return empty set

        // for all the data items in this site whose readAvailable is true, we send its committed value
    }

    public void fail() {

        // change the site status

        // remove all the locks on this site

        // set the latestFailedTime to current time

        // set the readAvailable of all the data copies as false
    }
    
    public void recover() {

        // change the site status

        // for all the non-replicated data copies on this site, set its readAvailable to true

    }
}