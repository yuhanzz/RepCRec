import java.util.*;

public class TransactionManager {
    
    int time;
    Map<Integer, DataManager> sites;
    Map<Integer, Transaction> transactions;
    // the set of sites(ID) that contains a copy of that variables(ID)
    Map<Integer, Set<Integer>> availableSites;
    List<Operation> pendingOperations;
    List<Operation> readyOperations;
    Map<Integer, Set<Integer>> waitsForGraph;

    /**
     * 1. call deadLockDetection()
     * 2. call this.nextOperation()
     * 3. if the transaction status is not blocked, try to execute the operation by calling read, write, ...
     * 4. if failed, put the operation in pending list, go to step two 
     */
    public void execute()
    {

    }

    // update transactions list, call site.read() for each variable to acquire the snapshot of the database, return false if any read failed
    public boolean beginReadOnly(int transactionID)
    {

    }

    // update transactions list
    public boolean begin(int transactionID)
    {

    }

    /**
     * check the commit condition by calling canCommit() for each accessed site
     * if can commit, then just call site.commit(transactionID) for each accessed site
     * else, call abort();
     * update the waitForGraph and transaction status
     */
    public boolean commit(int transactionID)
    {

    }

    // call site.abort(int transactionID) for each accessed site, and update the waitForGraph and transaction status
    public boolean abort(int transactionID)
    {

    }

    /**
     * if it's a read only transaction, just read from its snapshot
     * otherwise try to require a read lock from any of the available sites by trying to call site.acquireLock() and then site.read()
     * if failed to acquire the lock due to lock conflict(return value is greater than 0), add the information in waitsForGraph
     */
    public boolean read(int transactionID, int variableID)
    {

    }

    /**
     * try to acquire write locks from the necessary sites by calling site.acquireLock() and then site.write()
     * if there is lock conflict, add the information in waitsForGraph, and return false
     * if there is site failure, then just ignore that site and write to the active site, and still return true
     */
    public boolean write(int transactionID, int variableID, int value)
    {

    }

    // will call site.fail()
    public void fail(int siteID)
    {

    }

    // will call site.recover()
    public void recover(int siteID)
    {

    }

    // check the cycle in graph, if there is cycle, abort the youngest transaction and check again
    public void deadLockDetection()
    {

    }

    // will call site.dump() on each Site
    public void dump()
    {

    }

    // for debugging purpose, will also call querystate() on each DM
    public void querystate()
    {

    }

    // For the similation part
    public boolean readFromFile()
    {

    }
    public File executionFile()
    {

    }
    /**
     * 1. check the waitsForGraph according to pending list, if the waiting locks set is empty, then put this operation to ready list
     * 1. if the ready list is not empty, then pop the first ready operation, else read a new operation from the file/std 
     * 2. if both are empty, then call this.shutDown()
     */
    public Operation nextOperation()
    {

    }

    // will close the executionFile if readFromFile is true
    public void shutDown()
    {

    }

    // initialize the transactionManager (two ways: read from file / read from std)
    TransactionManager(List<DataManager> sites, String filePath);
    TransactionManager(List<DataManager> sites);
}
