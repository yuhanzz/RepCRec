package src.main.java;

public class Operation {
    private OperationType type;
    private int transactionId;
    private int variableId;
    private int valueToWrite;
    private int arrivingTime;

    // for begin, begin_read_only, commit, dump
    public Operation(OperationType type, int transactionId, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.variableId = -1;
        this.arrivingTime = arrivingTime;
    }

    // for read
    public Operation(OperationType type, int transactionId, int variableId, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.variableId = variableId;
        this.arrivingTime = arrivingTime;
    }

    // for write
    public Operation(OperationType type, int transactionId, int variableId, int valueToWrite, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.variableId = variableId;
        this.valueToWrite = valueToWrite;
        this.arrivingTime = arrivingTime;
    }

    public OperationType getType() {
        return type;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public int getVariableId() {
        return variableId;
    }

    public int getValueToWrite() {
        return valueToWrite;
    }

    public int getArrivingTime() {
        return arrivingTime;
    }
}
