import java.util.*;
public class TransactionManager {

    private Map<Integer, Site> sites;    // <key : siteId, value : site>
    private Map<Integer, Transaction> transactions; // <key : transactionId, value : transaction>
    private Map<Integer, DataInfo> dataLocation;  // <key : variableId, value : data information>
    private List<Operation> pendingList;
    private Map<Integer, Set<Integer>> waitsForGraph;
    private Map<Integer, Map<Integer, Integer>> snapshots;  // the key is the transaction id, the value is the snapshot


    /**
     * if the read is successful, will return true, and output the message, might change the lock status
     * if the read failed , will return false, might change the waitsForGraph
     * may change a transaction's accessedSite
     * will set new status for transaction
     */
    public boolean read(Operation operation, int currentTime) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        Transaction transaction = transactions.get(transactionId);
        DataInfo dataInfo = dataLocation.get(variableId);
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

        // check whether there is any earlier pending operations waiting for lock on this variable
        for (Operation pendingOperation : pendingList) {
            if (pendingOperation.getVariableId() == variableId && pendingOperation.getArrivingTime() < operation.getArrivingTime()) {
                transaction.setStatus(TransactionStatus.BLOCKED);
                return false;
            }
        }

        // check whether could read an available copy
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
            Set<Integer> conflictingTransactions = site.lockAvailable(LockType.READ, variableId);

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
    public boolean write(Operation operation, int currentTime) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        int value = operation.getValueToWrite();
        Transaction transaction = transactions.get(transactionId);
        DataInfo dataInfo = dataLocation.get(variableId);
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

        // check whether there is any earlier pending operations waiting for lock on this variable
        for (Operation pendingOperation : pendingList) {
            if (pendingOperation.getVariableId() == variableId && pendingOperation.getArrivingTime() < operation.getArrivingTime()) {
                transaction.setStatus(TransactionStatus.BLOCKED);
                return false;
            }
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

            Set<Integer> conflictingTransactions = site.lockAvailable(LockType.WRITE, variableId);

            // if can not acquire write lock, add all conflicting transactions to the waitsForGraph
            if (!conflictingTransactions.isEmpty() && !(conflictingTransactions.size() == 1 && conflictingTransactions.contains(transactionId))) {
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
    public boolean commit(int transactionId) {
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

        // if can not commit
        if (!canCommit) {
            return false;
        }

        // if can commit, commit the transaction on every site
        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            site.commit(transactionId);
        }

        // successfully committed
        removeTransactionFromWaitsForGraph(transactionId);
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
        removeTransactionFromWaitsForGraph(transactionId);
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
     * return the current time
    */
    public int receiveRecoverNotice(int siteId, int currentTime) {
        Site site = sites.get(siteId);
        Map<Integer, Integer> newSnapshot = site.takeSnapShot();

        if (newSnapshot.isEmpty()) {
            return currentTime;
        }
        
        // Make up the missing part of the snapshots that are already taken
        for (int transactionId : snapshots.keySet()) {

            Map<Integer, Integer> takenSnapshot = snapshots.get(transactionId);

            for (int variableId : newSnapshot.keySet()) {
                // only update the snapshot if this variable in that snapshot is missing
                if (!takenSnapshot.containsKey(variableId)) {
                    takenSnapshot.put(variableId, newSnapshot.get(variableId));
                }
            }
        }

        return retry(currentTime);
    }

    /**
     * 
     */
    public boolean execute(Operation operation, int currentTime) {
        boolean executionSuccessful = true;
        switch(operation.getType()) {
            case BEGIN:
                begin(operation.getTransactionId(), currentTime);
                break;
            case BEGIN_READ_ONLY:
                beginRO(operation.getTransactionId(), currentTime);
                break;
            case COMMIT:
                {
                    boolean commitSuccessful = commit(operation.getTransactionId());
                    if (!commitSuccessful) {
                        abort(operation.getTransactionId());
                    }
                }
                break;
            case READ:
                executionSuccessful = read(operation, currentTime);
                break;
            case WRITE:
                executionSuccessful = write(operation, currentTime);
                break;
        }

        return executionSuccessful;
    }

    /**
     * return the time to finish this request
     */
    public int handleRequest(Operation operation, int currentTime) {
        Transaction transaction = transactions.get(operation.getTransactionId());

        // if the transaction is currently blocked, add this operation to pending list
        if (transaction.getStatus() == TransactionStatus.BLOCKED) {
            pendingList.add(operation);
            return currentTime;
        }

        // call execute
        boolean executionSuccessful = execute(operation, currentTime);

        if (executionSuccessful) {
            // if execution is successful, increase current time, and retry because there could be potential unblocked operations
            return retry(currentTime + 1);
        } else {
            // if execution is not successful, add the operation to pending list
            pendingList.add(operation);
            return currentTime;
        }
    }

    /**
     * iterate through the pending list and retry, and return the time after retry
     */
    public int retry(int currentTime) {
        // maintain a set of transaction that are still blcoked, initally the set is empty
        Set<Integer> remainBlockedTransactions = new HashSet<>();

        // the finished operations
        Set<Operation> finishedOperations = new HashSet<>();

        // retry every operation that is not in blocked set by calling execute
        for (Operation operation : pendingList) {
            int transactionId = operation.getTransactionId();

            if (remainBlockedTransactions.contains(transactionId)) {
                continue;
            }

            boolean retrySuccessful = execute(operation, currentTime);

            if (retrySuccessful) {
                currentTime++;
                finishedOperations.add(operation);
            } else {
                // if retry failed, add its transaction to blocked set
                remainBlockedTransactions.add(transactionId);
            }

        }

        // remove all the finished operations from pending list
        for (Operation operation : finishedOperations) {
            pendingList.remove(operation);
        }

        return currentTime;
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
        boolean hasCycle = true; 
        while(hasCycle)
        {
            Set<Integer> cycle = detect(waitsForGraph);
            if(cycle.isEmpty())
            {
                hasCycle = false;
            }
            else
            {
                int victim = -1;
                int minTime = Integer.MAX_VALUE; 
                for(Integer transaction: cycle)
                {
                    if(transactions.get(transaction).getBeginTime() < minTime)
                    {
                        victim = transaction;
                        minTime = transactions.get(transaction).getBeginTime();
                    }
                }
                abort(victim);
            }
        }
        
    }

    public Set<Integer> detect(Map<Integer, Set<Integer>> transactions)
    {
        Map<Integer,Integer> degree = new HashMap<>();
        Map<Integer,Set<Integer>> children = new HashMap<>();

        for (Integer tx : transactions.keySet())
        {
            degree.put(tx, transactions.get(tx).size());
            if (!children.containsKey(tx))
            {
                children.put(tx, new HashSet<>());
            }

            for (Integer parent : transactions.get(tx))
            {
                Set<Integer> dependent = children.getOrDefault(parent, new HashSet<>());
                dependent.add(tx);
            }
        }

        Queue<Integer> queue = new LinkedList<>();
        for (Integer key : children.keySet())
        {
            if (degree.get(key) == 0)
            {
                queue.offer(key);
            }
        }

        int count = queue.size();

        while (!queue.isEmpty())
        {
            int tx = queue.poll();
            for (Integer child : children.get(tx))
            {
                int d = degree.get(child);
                if (d > 0)
                {
                    degree.put(child, d - 1);
                    if (d == 1)
                    {
                        queue.offer(child);
                        count++;
                    }
                }
            }
        }

        Set<Integer> result = new HashSet<>();
        if (count < transactions.size())
        {
            for (Integer key : degree.keySet())
            {
                if (degree.get(key) != 0)
                {
                    result.add(key);
                }
            }
        }

        return result;
    }

    /**
     * initialize TransactionManager
     */
    public TransactionManager(Map<Integer, Site> sites) {

        this.sites = sites;
        transactions = new HashMap<>();
        dataLocation = new HashMap<>();
        pendingList = new ArrayList<>();
        waitsForGraph = new HashMap<>();
        snapshots = new HashMap<>();

        // initialize data location information
        for (int i = 1; i <= 20; i++) {
            List<Integer> availableSites = new ArrayList<>();
            DataInfo dataInfo;
            if (i % 2 == 0) {
                for (int j = 1; j <= 10; j++) {
                    availableSites.add(j);
                }
                dataInfo = new DataInfo(i, DataType.REPLICATED, availableSites);
            } else {
                availableSites.add(1 + i % 10);
                dataInfo = new DataInfo(i, DataType.NOT_REPLICATED, availableSites);
            }
            dataLocation.put(i, dataInfo);
        }
    }

}