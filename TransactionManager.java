public class TransactionManager {

    Map<Integer, Site> sites;    // <key : siteId, value : site>
    Map<Integer, Transaction> transactions; // <key : transactionId, value : transaction>
    Map<Integer, DataInfo> dataCollection;  // <key : variableId, value : data information>
    List<Operation> pendingList;
    Map<Integer, Set<Integer>> waitsForGraph;
    Map<Integer, Map<Integer, Integer>> snapshots;  // the key is the timestamp, the value is the snapshot


    public boolean read() {
        
    }

    public boolean write() {

    }

    public boolean commit() {

    }

    public void abort() {

    }

    public void begin() {

    }

    public void beginRO() {
        // take snapshot
    }

    /**
     * make up the missing part of each snapshot immediately
    */
    public void receiveRecoverNotice() {

    }

    /**
     * responsible for updating the transaction status
     * responsible for updating waitsForGraph
     */
    public boolean execute(Operation operation) {

        // call the corresponding method(read/write/commit ...if commit failed, call abort )

        // update the waitsForGraph accordingly (add edge if read/write failed, remove edge if commit/abort)

        // update the transaction status

        // return false if read / write failed
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