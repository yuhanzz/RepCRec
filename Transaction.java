import java.util.*;

public class Transaction{
    public int ID;
    public int beginTime;
    public TransactionType type;   // READ_ONLY, READ_WRITE
    public TransactionStatus status;   // ACTIVE, ABORTED, COMMITED, BLOCKED
    public List<Integer> accessedSites;
    public Map<Integer, Integer> snapshot;    // only used for READ_ONLY transaction
    public Set<Lock> locks;
}