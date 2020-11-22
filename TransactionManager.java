public class TransactionManager {

    private Map<Integer, Site> sites;    // <key : siteId, value : site>
    private Map<Integer, Transaction> transactions; // <key : transactionId, value : transaction>
    private Map<Integer, DataInfo> dataCollection;  // <key : variableId, value : data information>
    private List<Operation> pendingList;
    private Map<Integer, Set<Integer>> waitsForGraph;
    private Map<Integer, Map<Integer, Integer>> snapshots;  // the key is the transaction id, the value is the snapshot


    /**
     * if the read is successful, will return true, and output the message, might change the lock status
     * if the read failed , will return false, might change the waitsForGraph
     * may change a transaction's accessedSite
     * will set new status for transaction
     */
    public boolean read(int transactionId, int variableId, int currentTime) {
        Transaction transaction = transactions.get(transactionId);
        DataInfo dataInfo = dataCollection.get(variableId);
        List<Integer> availableSites = dataInfo.getAvailableSites();
        Integer value = null;

        // if read-only transaction
        if (transaction.isReadOnly()) {
            Map<Integer, Integer> snapshot = snapshots.get(transactionId);
            // if snapshot is missing this part
            if (!snapshot.containsKey(variableId)) {
                transaction.setStatus(TransactionStatus.BLOCKED);
                return false;
            }
            value = snapshot.get(variableId);
            System.out.println("x" + variableId + ": " + value);
            transaction.setStatus(TransactionStatus.ACTIVE);
            return true;
        }

        // if read-write transaction
        for (int siteId : availableSites) {
            Site site = sites.get(siteId);

            // if the site is down
            if (site.getSiteStatus() == SiteStatus.DOWN) {
                continue;
            }

            // if the copy is not available for read
            if (!site.readAvailable(variableId)) {
                continue;
            }

            // if holding the read lock
            if (transaction.isHoldingLock(LockType.READ, variableId)) {
                value = site.read(variableId);
                break;
            }

            // if need to acquire read lock
            Set<Integer> conflictingTransactions = lockAvailable(LockType.READ, variableId)

            // if can not acquire read lock
            if (!conflictingTransactions.isEmpty()) {
                Set<Integer> vertices = waitsForGraph.getOrDefault(transactionId, new HashSet<>());
                for (int vertex : conflictingTransactions) {
                    vertices.add(vertex);
                }
                waitsForGraph.put(transactionId, vertices);
                break;
            }

            // if can acquire read lock
            site.acquireLock(LockType.READ, transactionId, variableId);
            transaction.addAccessedSite(currentTime, siteId);
            transaction.addLock(LockType.READ, variableId);
            value = site.read(variableId);
            break;
        }

        if (value == null) {
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        System.out.println("x" + variableId + ": " + value);
        transaction.setStatus(TransactionStatus.ACTIVE);
        return true;
    }

    /**
     * write to all the available copies
     * return false if waiting for locks or all sites are down
     * may change the waitsForGraph, and transaction's holding locks
     * may change a transaction's accessedSite
     * will set new status for transaction
     */
    public boolean write(int transactionId, int variableId, int value, int currentTime) {
        Transaction transaction = transactions.get(transactionId);
        DataInfo dataInfo = dataCollection.get(variableId);
        List<Integer> availableSites = dataInfo.getAvailableSites();
        int upSites = 0;

        // if is holding the lock, just write to all available sites, if all the sites failed, return false
        if (transaction.isHoldingLock(LockType.WRITE, variableId)) {

            for (int siteId : availableSites) {
                Site site = sites.get(siteId);

                // if the site is down
                if (site.getSiteStatus() == SiteStatus.DOWN) {
                    continue;
                }

                site.write(variableId, value);
                upSites++;
            }
            if (upSites > 0) {
                transaction.setStatus(TransactionStatus.ACTIVE);
                return true;
            }
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // check write lock availability
        boolean writeLockAvailable = true;
        for (int siteId : availableSites) {
            Site site = sites.get(siteId);

            // if the site is down
            if (site.getSiteStatus() == SiteStatus.DOWN) {
                continue;
            }

            upSites++;

            Set<Integer> conflictingTransactions = lockAvailable(LockType.WRITE, variableId)

            // if can not acquire write lock, add all conflicting transactions to the waitsForGraph
            if (!conflictingTransactions.isEmpty()) {
                writeLockAvailable = false;
                Set<Integer> vertices = waitsForGraph.getOrDefault(transactionId, new HashSet<>());
                for (int vertex : conflictingTransactions) {
                    vertices.add(vertex);
                }
                waitsForGraph.put(transactionId, vertices);
            }
        }

        if (!writeLockAvailable) {
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        if (upSites == 0) {
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // if can acquire write lock
        for (int siteId : availableSites) {
            Site site = sites.get(siteId);
            if (site.getSiteStatus() == SiteStatus.DOWN) {
                continue;
            }
            site.acquireLock(LockType.WRITE, transactionId, variableId);
            transaction.addAccessedSite(currentTime, siteId);
            site.write(variableId, value);
        }

        transaction.addLock(LockType.WRITE, variableId);
        transaction.setStatus(TransactionStatus.ACTIVE);
        return true;
    }

    /**
     * two phase commit
     * will change waitsForGraph
     * will set new status for transaction
     */
    public boolean commit(int transactionId, int currentTime) {
        Transaction transaction = transactions.get(transactionId);
        Map<Integer, Integer> accessedSites = transaction.getAccessedSites();

        // if read-only transaction
        if (transaction.isReadOnly()) {
            transaction.setStatus(TransactionStatus.COMMITED);
            return true;
        }

        // if read-write transaction, two phase commit
        boolean canCommit = true;

        // concensus
        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            int firstAccessTime = accessedSites.get(siteId);
            if (!site.commitReady(firstAccessTime)) {
                canCommit = false;
                break;
            }
        }

        // if can not commit, abort
        if (!canCommit) {
            abort(transactionId);
            return false;
        }

        // successfully committed
        removeTransactionFromWaitsForGraph(transactionId)
        transaction.setStatus(TransactionStatus.COMMITED);
        return true;
    }

    /**
     * will change waitsForGraph
     * will set new status for transaction
     */
    public void abort(int transactionId) {
        Transaction transaction = transactions.get(transactionId);
        Map<Integer, Integer> accessedSites = transaction.getAccessedSites();

        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            site.abort(transactionId);
        }
        removeTransactionFromWaitsForGraph(transactionId)
        transaction.setStatus(TransactionStatus.ABORTED);
    }

    public void begin(int transactionId, int currentTime) {
        Transaction transaction = new Transaction(transactionId, currentTime, TransactionType.READ_WRITE);
        transactions.put(transactionId, transaction);
    }

    public void beginRO(int transactionId, int currentTime) {
        Transaction transaction = new Transaction(transactionId, currentTime, TransactionType.READ_ONLY);
        transactions.put(transactionId, transaction);

        // take snapshot
        Map<Integer, Integer> snapshot = new HashMap<>();
        for (int siteId : sites.keySet()) {
            Site site = sites.get(siteId);
            Map<Integer, Integer> snapshotFromEachSite = site.takeSnapShot();
            for (int variableId : snapshotFromEachSite.keySet()) {
                snapshot.put(variableId, snapshotFromEachSite.get(variableId));
            }
        }
        snapshots.put(transactionId, snapshot);
    }

    /**
     * make up the missing part of each snapshot immediately when received site recover notice
    */
    public void receiveRecoverNotice(int siteId) {
        Site site = sites.get(siteId);
        Map<Integer, Integer> newSnapshot = site.takeSnapShot();

        if (newSnapshot.isEmpty()) {
            return;
        }
        
        // Make up the missing part of the snapshots that are already taken
        for (int trasacntionId : snapshots.entrySet()) {

            Map<Integer, Integer> takenSnapshot = snapshots.get(trasacntionId);

            for (int variableId : newSnapshot.keySet()) {
                // only update the snapshot if this variable in that snapshot is missing
                if (!takenSnapshot.containsKey(variableId)) {
                    takenSnapshot.put(variableId, newSnapshot.get(variableId));
                }
            }
        }
    }

    /**
     * responsible for updating the transaction status
     * responsible for updating waitsForGraph
     */
    public boolean execute(Operation operation) {

        // call the corresponding method(read/write/commit ...if commit failed, call abort )

        // update the waitsForGraph accordingly (add edge if read/write failed, remove edge if commit/abort)


        // return false if read / write failed
        
        // retry if necessary

    }

    public void handleRequest() {

        // if transaction status is blocked, then add the operation to pending list and return

        // call execute

        // if execute return false, mark this transaction as BLOCKED, and add the operation to pending list
    }

    /**
     * iterate through the pending list and retry 
     */
    public void retry() {
        // maintain a set of blocked transaction, initally the set is empty
        
        // retry every operation that is not in blocked set by calling execute
        
        // if retry failed, add its transaction to blocked set

        // if retry succeeded, change transaction status to ACTIVE, remove this operation from the pending list 
    }

    /**
     * remove the edges in waitsForGraph whose source vertex or destination vertex is this transaction
     */
    public void removeTransactionFromWaitsForGraph(int transactionId) {
        waitsForGraph.remove(transactionId);
        for (int sourceVertex : waitsForGraph.keySet()) {
            Set<Integer> destinationVertice = waitsForGraph.get(sourceVertex);
            destinationVertice.remove(transactionId);
        }
    }

    public void deadLockDetection() {

    }

    /**
     * initialize TransactionManager
     */
    public void TransactionManager(Map<Integer, Site> sites) {

        // initialize data information (dataCollection)

        // initialize sites
    }

}