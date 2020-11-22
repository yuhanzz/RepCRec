import java.util.*;
import javafx.util.Pair;
public class LockManager {

    private Map<Integer,Set<Pair<Integer,LockType>>> lockTable;

    public LockManager(){
        this.lockTable = new HashMap<>();
    }

    public Set<Integer> getAllTrasactionIDS(int variableID)
    {
        Set<Integer> transactionIDS = new HashSet<>();
        Set<Pair<Integer,LockType>> list = lockTable.get(variableID);
        for(Pair<Integer,LockType> pair: list)
        {
            transactionIDS.add(pair.getKey());
        }
        return transactionIDS;

    }

    public void addLock(LockType lockType, int transactionId, int variableId)
    {
        Set<Pair<Integer,LockType>> set = lockTable.get(variableId);
        Pair<Integer,LockType> pair = new Pair<>(transactionId,lockType);
        if(!set.contains(pair))
        {
            set.add(pair);
        }
    }

    /**
     * return the set of variables that this transaction has write lock on
     */
    public Set<Integer> getAllWrittenVariables(int transactionId) {
        Set<Integer> set = new HashSet<>(); 
        Set<Integer> variableIds= lockTable.keySet(); 
        for(Integer variableId: variableIds)
        {
            Set<Pair<Integer,LockType>> locks = lockTable.get(variableId);
            for(Pair<Integer,LockType>> p: locks)
            {
                if(p.getKey() == transactionId && p.getValue() == LockType.WRITE)
                {
                    set.add(variableId);
                }
            }
        }
        return set;
    }

    /**
     * release all the locks that this transaction has
     */
    void releaseAllLocks(int transactionId) {
        Set<Map.Entry<Integer, Set<Pair<Integer,LockType>>>> entrySet = lockTable.entrySet();
        for(Map.Entry<Integer, Set<Pair<Integer,LockType>>> entry: entrySet)
        {
            Set<Pair<Integer,LockType>> locks = new HashSet<>(entry.getValue());
            for(Pair<Integer,LockType> lock: entry.getValue())
            {
                if(lock.getKey() == transactionId)
                {
                    locks.remove(lock);
                }
            }
            lockTable.put(entry.getKey(),locks);
        }
    }

    /**
     * clear the whole lock table due to site failure
     */
    public void clear() {
        lockTable.clear();
    }
}