/**
 * records all the information that the transaction manager needs to know about a transaction
 */
public class Transaction{
    public int id;
    public int beginTime;
    public TransactionType type;
    public TransactionStatus status;
    public Set<Integer> accessedSites;
    public Set<Integer, Pair<Integer, Integer>> holdingLocks;
}