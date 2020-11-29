package src.main.java;

import javafx.util.Pair;

import java.util.*;

public class TransactionManager {

    private Map<Integer, Site> sites;    // <key : siteId, value : site>
    private OutputPrinter outputPrinter;
    private Map<Integer, Transaction> transactions; // <key : transactionId, value : transaction>
    private Map<Integer, DataInfo> dataLocation;  // <key : variableId, value : data distribution information>
    private List<Operation> pendingList;
    private Map<Integer, Set<Integer>> waitsForGraph;
    private Map<Integer, List<Integer>> failureHistory;

    /**
     * Execute a general read operation
     * side effect: might change transaction status
     * @param operation the read operation
     * @param currentTime current time
     * @return true if the read is successful, false if blocked
     * @author Yuhan Zhou 
     */
    public boolean read(Operation operation, int currentTime) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        Transaction transaction = transactions.get(transactionId);

        // if the local cache has this variable, read is successful
        if (transaction.getLocalCache().containsKey(variableId)) {
            int value = transaction.read(variableId);
            transaction.setStatus(TransactionStatus.ACTIVE);
            outputPrinter.printReadSuccess(variableId, value, transactionId);
            return true;
        }

        // otherwise, try to read from sites
        if (transaction.isReadOnly()) {
            return read_RO(operation);
        }
        return read_RW(operation, currentTime);
    }

    /**
     * Attempt to read an available copy for read-write transaction
     * side effect: will change the local cache and info of transactions, the lock table, and waitsForGraph
     * @param operation the read operation
     * @param currentTime current time
     * @return true if the read is successful, false if blocked
     * @author Yuhan Zhou 
     */
    public boolean read_RW(Operation operation, int currentTime) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        Transaction transaction = transactions.get(transactionId);

        // firstly check if there is any blocking transactions in pending list to prevent starvation
        Set<Integer> blockingTransactions = getBlockingTransaction(operation);
        if (!blockingTransactions.isEmpty()) {
            addEdgesToWaitsForGraph(transactionId, blockingTransactions);
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // if there is no blocking transactions, try to acquire lock
        DataInfo dataInfo = dataLocation.get(variableId);
        List<Integer> availableSites = dataInfo.getAvailableSites();

        for (int siteId : availableSites) {
            Site site = sites.get(siteId);
            DataManager dataManager = site.getDataManager();
            LockManager lockManager = site.getLockManager();

            // if the site is down or the copy is not available for read
            if (!site.isUp() || !dataManager.readAvailable(variableId)) {
                continue;
            }

            // if find an available site, try to acquire the read lock
            Set<Integer> conflictingTransactions = lockManager.acquireLock(transactionId, variableId, LockType.READ);

            // if can not acquire read lock
            if (!conflictingTransactions.isEmpty()) {
                addEdgesToWaitsForGraph(transactionId, conflictingTransactions);
                transaction.setStatus(TransactionStatus.BLOCKED);
                return false;
            }

            // if acquire read lock successfully, read the value into local cache
            transaction.addLock(LockType.READ, variableId);
            int value = dataManager.read(variableId);
            transaction.cache(variableId, value);
            transaction.addAccessedSite(currentTime, siteId);
            transaction.setStatus(TransactionStatus.ACTIVE);
            outputPrinter.printReadSuccess(variableId, value, transactionId);
            return true;
        }

        // if read failed due to all sites unavailable
        transaction.setStatus(TransactionStatus.BLOCKED);
        return false;
    }

    /**
     * Attempt to read a snapshot for read-only transaction
     * side effect: will change the local cache and info of transactions
     * @param operation the read operation
     * @return true if the read is successful, false if blocked
     * @author Yuhan Zhou 
     */
    public boolean read_RO(Operation operation) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        Transaction transaction = transactions.get(transactionId);
        int transactionBeginTime = transaction.getBeginTime();

        DataInfo dataInfo = dataLocation.get(variableId);
        List<Integer> availableSites = dataInfo.getAvailableSites();

        for (int siteId : availableSites) {
            Site site = sites.get(siteId);
            DataManager dataManager = site.getDataManager();

            // if the site is down
            if (!site.isUp()) {
                continue;
            }

            Pair<Integer, Integer> snapshot = dataManager.getSnapshot(variableId, transactionBeginTime);
            int commitTime = snapshot.getKey();
            int commitValue = snapshot.getValue();

            // if there is failure occurred between the latest commit time and the transaction begin time, this version can not be read
            if (hasFailureBetween(siteId, commitTime, transactionBeginTime)) {
                continue;
            }

            // read success
            transaction.cache(variableId, commitValue);
            transaction.setStatus(TransactionStatus.ACTIVE);
            outputPrinter.printReadSuccess(variableId, commitValue, transactionId);
            return true;
        }

        // read blocked
        transaction.setStatus(TransactionStatus.BLOCKED);
        return false;
    }

    /**
     * Attempt to write a variable
     * side effect: will change the local cache and info of transactions, the lock table, and waitsForGraph
     * @param operation the write operation
     * @return
     * @author Yuhan Zhou 
     */
    public boolean write(Operation operation, int currentTime) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        int value = operation.getValueToWrite();
        Transaction transaction = transactions.get(transactionId);

        // if is holding the write lock, write is successful
        if (transaction.isHoldingLock(LockType.WRITE, variableId)) {
            transaction.cache(variableId, value);
            transaction.setStatus(TransactionStatus.ACTIVE);
            outputPrinter.printWriteSuccess(variableId, value, transactionId);
            return true;
        }

        // if need to acquire lock, firstly check if there is any blocking transactions in pending list to prevent starvation
        Set<Integer> blockingTransactions = getBlockingTransaction(operation);
        if (!blockingTransactions.isEmpty()) {
            addEdgesToWaitsForGraph(transactionId, blockingTransactions);
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // otherwise, try to acquire write lock
        DataInfo dataInfo = dataLocation.get(variableId);
        List<Integer> availableSites = dataInfo.getAvailableSites();
        Set<Integer> accessedSites = new HashSet<>();

        boolean writeLockAvailable = true;
        for (int siteId : availableSites) {
            Site site = sites.get(siteId);
            LockManager lockManager = site.getLockManager();

            // if the site is down
            if (!site.isUp()) {
                continue;
            }

            accessedSites.add(siteId);

            Set<Integer> conflictingTransactions = lockManager.acquireLock(transactionId, variableId, LockType.WRITE);

            // if can not acquire write lock, add all conflicting transactions to the waitsForGraph
            if (!conflictingTransactions.isEmpty() && !(conflictingTransactions.size() == 1 && conflictingTransactions.contains(transactionId))) {
                writeLockAvailable = false;
                addEdgesToWaitsForGraph(transactionId, conflictingTransactions);
            }
        }

        // if failed to acquire write lock on all available sites, release the locks that already obtained
        if (!writeLockAvailable) {
            boolean holdingReadLock = transaction.isHoldingLock(LockType.READ, variableId);
            for (int siteId : availableSites) {
                Site site = sites.get(siteId);
                LockManager lockManager = site.getLockManager();

                // if the site is down
                if (!site.isUp()) {
                    continue;
                }

                lockManager.releaseWriteLock(transactionId, variableId, holdingReadLock);

            }

            // write failed
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // if there is no site up, also failed
        if (accessedSites.isEmpty()) {
            transaction.setStatus(TransactionStatus.BLOCKED);
            return false;
        }

        // if all write locks acquired, write success
        for (int siteId : accessedSites) {
            transaction.addAccessedSite(currentTime, siteId);
        }
        transaction.addLock(LockType.WRITE, variableId);
        transaction.cache(variableId, value);
        transaction.setStatus(TransactionStatus.ACTIVE);
        outputPrinter.printWriteSuccess(variableId, value, transactionId);
        return true;
    }

    /**
     * Attempt to commit a transaction
     * side effect: will change the status of transactions, the lock manager, the data manager, and waitsForGraph
     * @param transactionId the transaction to commit
     * @param currentTime current time
     * @return true if can commit, false if can not
     * @author Yuhan Zhou 
     */
    public boolean commit(int transactionId, int currentTime) {
        Transaction transaction = transactions.get(transactionId);
        Map<Integer, Integer> accessedSites = transaction.getAccessedSites();

        // if read-only transaction
        if (transaction.isReadOnly()) {
            transaction.setStatus(TransactionStatus.COMMITED);
            outputPrinter.printCommitSuccess(transactionId);
            return true;
        }

        // if read-write transaction, two phase commit
        boolean canCommit = true;

        // consensus
        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            int firstAccessTime = accessedSites.get(siteId);
            if (hasFailureBetween(siteId, firstAccessTime, currentTime)) {
                canCommit = false;
                break;
            }
        }

        // if can not commit
        if (!canCommit) {
            return false;
        }

        // if can commit, firstly get all the written variables
        Map<Integer, Integer> updatedVariables = new HashMap<>();
        Map<Integer, Integer> localCache = transaction.getLocalCache();
        for (int variableId : localCache.keySet()) {
            if (transaction.isHoldingLock(LockType.WRITE, variableId)) {
                updatedVariables.put(variableId, localCache.get(variableId));
            }
        }

        // commit on every site
        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            site.commit(transactionId, currentTime, updatedVariables);
        }

        // successfully committed
        removeTransactionFromWaitsForGraph(transactionId);
        transaction.setStatus(TransactionStatus.COMMITED);
        outputPrinter.printCommitSuccess(transactionId);
        return true;
    }

    /**
     * Abort the transaction
     * side effect: will change lock manager, transaction status, and waitsForGraph
     * @param transactionId the transaction to abort
     * @author Yuhan Zhou 
     */
    public void abort(int transactionId) {
        Transaction transaction = transactions.get(transactionId);
        Map<Integer, Integer> accessedSites = transaction.getAccessedSites();

        for (int siteId : accessedSites.keySet()) {
            Site site = sites.get(siteId);
            if (site.isUp()) {
                site.abort(transactionId);
            }
        }
        removeTransactionFromWaitsForGraph(transactionId);
        transaction.setStatus(TransactionStatus.ABORTED);
        outputPrinter.printAbortSuccess(transactionId);
    }

    /**
     * Begin a read-write transaction
     * side effect: will change transactions
     * @param operation the begin operation
     * @author Yuhan Zhou 
     */
    public void begin(Operation operation) {
        int transactionId = operation.getTransactionId();
        int time = operation.getArrivingTime();
        Transaction transaction = new Transaction(transactionId, time, TransactionType.READ_WRITE);
        transactions.put(transactionId, transaction);
    }

    /**
     * Begin a read-only transaction
     * side effect: will change transactions
     * @param operation the begin opeartion
     */
    public void beginRO(Operation operation) {
        int transactionId = operation.getTransactionId();
        int time = operation.getArrivingTime();
        Transaction transaction = new Transaction(transactionId, time, TransactionType.READ_ONLY);
        transactions.put(transactionId, transaction);
    }

    /**
     * Attempt to execute an operation
     * @param operation the operation to execute
     * @param currentTime the current time
     * @return true if the execution is successful, false if not blocked
     * @author Yuhan Zhou 
     */
    public boolean execute(Operation operation, int currentTime) {
        boolean executionSuccessful = true;
        switch(operation.getType()) {
            case BEGIN:
                begin(operation);
                break;
            case BEGIN_READ_ONLY:
                beginRO(operation);
                break;
            case COMMIT:
                {
                    boolean commitSuccessful = commit(operation.getTransactionId(), currentTime);
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
     * Handle the new request coming from std or file
     * side effect: might change pending list
     * @param operation the operation
     * @param currentTime the current time
     * @author Yuhan Zhou 
     */
    public void handleNewRequest(Operation operation, int currentTime) {
        Transaction transaction = transactions.get(operation.getTransactionId());

        // if the transaction is currently blocked, add this operation to pending list
        if (transaction != null && transaction.getStatus() == TransactionStatus.BLOCKED) {
            pendingList.add(operation);
        }

        // if the execution is not successful, add the operation to pending list
        if (!execute(operation, currentTime)) {
            pendingList.add(operation);
        }

    }

    /**
     * Iterate through the pending list and retry
     * @param currentTime the current time
     * @author Yuhan Zhou 
     */
    public void retry(int currentTime) {
        // maintain a set of transaction that are still blocked, initially the set is empty
        Set<Integer> remainBlockedTransactions = new HashSet<>();

        // the finished operations
        Set<Operation> finishedOperations = new HashSet<>();

        // retry every operation that is not in blocked set by calling execute
        for (Operation operation : pendingList) {
            int transactionId = operation.getTransactionId();

            if (remainBlockedTransactions.contains(transactionId)) {
                continue;
            }

            if (waitsForGraph.getOrDefault(transactionId, new HashSet<>()).size() > 0) {
                continue;
            }

            boolean retrySuccessful = execute(operation, currentTime);

            if (retrySuccessful) {
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
    }

    /**
     * Helper method for removing the edges in waitsForGraph whose source vertex or destination vertex is this transaction
     * side effect: will change the waitsForGraph
     * @param transactionId the transaction id
     * @author Yuhan Zhou 
     */
    public void removeTransactionFromWaitsForGraph(int transactionId) {
        waitsForGraph.remove(transactionId);
        Set<Integer> sourceToRemove = new HashSet<>();
        for (int sourceVertex : waitsForGraph.keySet()) {
            Set<Integer> destinationVertice = waitsForGraph.get(sourceVertex);
            destinationVertice.remove(transactionId);
            if (destinationVertice.isEmpty()) {
                sourceToRemove.add(sourceVertex);
            }
        }
        for (int source : sourceToRemove) {
            waitsForGraph.remove(source);
        }
    }

    /**
     * Add edges to waits for graph
     * @param source the source node
     * @param destinations the destination nodes
     * @author Yuhan Zhou 
     */
    private void addEdgesToWaitsForGraph(int source, Set<Integer> destinations) {
        Set<Integer> vertices = waitsForGraph.getOrDefault(source, new HashSet<>());
        for (int destination : destinations) {
            vertices.add(destination);
        }
        waitsForGraph.put(source, vertices);
    }

    /**
     * Deadlock detection
     * side effect: might abort transactions and change waitsForGraph
     * @return true if there is any cycle detected, false if not
     * @author Lillian Huang 
     */
    public boolean deadLockDetection() {
        queryState();
        boolean hasCycle = true;
        boolean detected = false;
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
                int maxTime = -1;
                for(Integer transaction: cycle)
                {
                    if(transactions.get(transaction).getBeginTime() > maxTime)
                    {
                        victim = transaction;
                        maxTime = transactions.get(transaction).getBeginTime();
                    }
                }
                outputPrinter.printDeadlock(victim);
                abort(victim);
                detected = true;
            }
        }
        return detected;
    }

    /**
     * Helper method for finding a cycle in waitsForGraph
     * @param graph the graph
     * @return the set of transactions in cycle
     * @author Lillian Huang 
     */
    public Set<Integer> detect(Map<Integer, Set<Integer>> graph)
    {
        Map<Integer,Integer> inDegree = new HashMap<>();
        Set<Integer> allNodes = new HashSet<>();

        for (int vertex : graph.keySet()) {
            allNodes.add(vertex);
            for (int destination : graph.get(vertex)) {
                allNodes.add(destination);
            }
        }

        for (int vertex : allNodes)
        {
            int degree = 0;
            for (int source : graph.keySet()) {
                if (graph.get(source).contains(vertex)) {
                    degree++;
                }
            }
            inDegree.put(vertex, degree);
        }

        Queue<Integer> queue = new LinkedList<>();
        for (int vertex : allNodes)
        {
            if (inDegree.get(vertex) == 0)
            {
                queue.offer(vertex);
            }
        }

        while (!queue.isEmpty())
        {
            int source = queue.poll();
            inDegree.remove(source);
            Set<Integer> destinations = graph.getOrDefault(source, new HashSet<>());
            for (int destination : destinations)
            {
                int degree = inDegree.get(destination);
                if (degree == 1) {
                    inDegree.remove(destination);
                    queue.offer(destination);
                } else {
                    inDegree.put(destination, degree - 1);
                }
            }
        }

        Set<Integer> cycle = inDegree.keySet();
        outputPrinter.printCycle(cycle);
        return cycle;
    }

    /**
     * Helper method for checking whether the site has a failure between a time range
     * @param siteId the site id
     * @param start the start of the time range
     * @param end the end of the time range
     * @return true if there is a failure history between the range, false if there isn't
     * @author Yuhan Zhou
     */
    private boolean hasFailureBetween(int siteId, int start, int end) {
        List<Integer> failures = failureHistory.getOrDefault(siteId, new ArrayList<>());
        for (int failure : failures) {
            if (failure > start && failure < end) {
                return true;
            }
        }
        return false;
    }

    /**
     * Initialize transaction manager
     * @param sites sites
     * @param outputPrinter output helper
     * @author Yuhan Zhou
     */
    public TransactionManager(Map<Integer, Site> sites, OutputPrinter outputPrinter) {

        this.sites = sites;
        this.outputPrinter = outputPrinter;
        transactions = new HashMap<>();
        dataLocation = new HashMap<>();
        pendingList = new ArrayList<>();
        waitsForGraph = new HashMap<>();
        failureHistory= new HashMap<>();

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

    /**
     * will print waitsForGraph in verbose mode
     * @author Yuhan Zhou
     */
    public void queryState() {
        outputPrinter.printWaitsForGraph(waitsForGraph);
    }

    /**
     * Add site failure record
     * @param siteId the failed site id
     * @param time the failed time
     * @author Yuhan Zhou
     */
    public void receiveFailureNotice(int siteId, int time) {
        List<Integer> history = failureHistory.getOrDefault(siteId, new ArrayList<>());
        history.add(time);
        failureHistory.put(siteId, history);
    }

    /**
     * Find the blocking transactions in pending list to prevent starvation
     * @param operation the operation
     * @return the set of transactions that the current transaction needs to wait for
     * @author Yuhan Zhou
     */
    private Set<Integer> getBlockingTransaction(Operation operation) {
        int transactionId = operation.getTransactionId();
        int variableId = operation.getVariableId();
        LockType locktype;
        if (operation.getType() == OperationType.WRITE) {
            locktype = LockType.WRITE;
        } else {
            locktype = LockType.READ;
        }

        Set<Integer> blockingTransactions = new HashSet<>();
        List<Pair<Integer, LockType>> list = new ArrayList<>();

        // add all the request waiting for the same variable to list
        for (Operation pendingOperation : pendingList) {
            // if already in pending list, return empty set
            if (pendingOperation == operation) {
                return new HashSet<>();
            }
            if (pendingOperation.getVariableId() == variableId) {
                if (transactions.get(pendingOperation.getTransactionId()).getType() == TransactionType.READ_ONLY) {
                    continue;
                }
                list.add(new Pair<>(pendingOperation.getTransactionId(), pendingOperation.getType() == OperationType.WRITE ? LockType.WRITE : LockType.READ));
            }
        }

        // if request for read lock, find the latest request for write lock
        if (locktype == LockType.READ) {
            for (int i = list.size() - 1; i >= 0; i--) {
                int currentTransaction = list.get(i).getKey();
                LockType currentLockType = list.get(i).getValue();
                if (currentLockType == LockType.WRITE) {
                    if (currentTransaction != transactionId) {
                        blockingTransactions.add(currentTransaction);
                    }
                    break;
                }
            }
            return blockingTransactions;
        }

        // if request for write lock, find the latest request for read locks or a latest request for write lock that blocks this write lock request

        // if there is no blocking transactions
        if (list.isEmpty()) {
            return blockingTransactions;
        }

        LockType blockingType = list.get(list.size() - 1).getValue();
        // if the write lock is blocked by a single write lock
        if (blockingType == LockType.WRITE) {
            blockingTransactions.add(list.get(list.size() - 1).getKey());
            return blockingTransactions;
        }
        // if the write lock is blocked by a set of read locks
        for (int i = list.size() - 1; i >= 0; i--) {
            int currentTransaction = list.get(i).getKey();
            LockType currentLockType = list.get(i).getValue();
            if (currentLockType == LockType.WRITE) {
                break;
            }
            if (currentTransaction != transactionId) {
                blockingTransactions.add(currentTransaction);
            }
        }
        return blockingTransactions;
    }

}