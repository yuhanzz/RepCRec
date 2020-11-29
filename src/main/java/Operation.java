package src.main.java;

public class Operation {
    private OperationType type;
    private int transactionId;
    private int variableId;
    private int valueToWrite;
    private int arrivingTime;

    /**
     * Constructor for begin, begin_read_only, commit
     * @param type Type of Operation (begin, begin_read_only, commit)
     * @param transactionId the transacationId for this operation 
     * @param arrivingTime the time this operation arrived 
     * @author Yuhan Zhou 
     */
    public Operation(OperationType type, int transactionId, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.arrivingTime = arrivingTime;
    }

    /**
     * Constructor for read 
     * @param type Type of Operation (read)
     * @param transactionId the transacationId for this operation 
     * @param variableId the variable the transaction wants to read 
     * @param arrivingTime the time this operation arrived 
     * @author Yuhan Zhou 
     */
    public Operation(OperationType type, int transactionId, int variableId, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.variableId = variableId;
        this.arrivingTime = arrivingTime;
    }

    /**
     * Constructor for write 
     * @param type Type of Operation (write)
     * @param transactionId the transacationId for this operation
     * @param variableId the variable the transaction wants to write to 
     * @param valueToWrite the value the transaction wants to write to the variable 
     * @param arrivingTime the time this operation arrived 
     * @author Yuhan Zhou 
     */
    public Operation(OperationType type, int transactionId, int variableId, int valueToWrite, int arrivingTime) {
        this.type = type;
        this.transactionId = transactionId;
        this.variableId = variableId;
        this.valueToWrite = valueToWrite;
        this.arrivingTime = arrivingTime;
    }

    /**
     * Getting the type of the Operation
     * @return the type of the Operation 
     * @author Yuhan Zhou 
     */
    public OperationType getType() {
        return type;
    }

    /**
     * Getting the transaction of the Operation 
     * @return the transaction of the Operation 
     * @author Yuhan Zhou 
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Getting the variable of the Operation 
     * @return the variable of the Operation 
     * @author Yuhan Zhou 
     */
    public int getVariableId() {
        return variableId;
    }

    /**
     * Getting the value that is being written to a variable of the Operation 
     * @return the value that is being written to a variable of the Operation 
     * @author Yuhan Zhou 
     */
    public int getValueToWrite() {
        return valueToWrite;
    }

    /**
     * Getting the time the operation arrived 
     * @return the time the operation arrived 
     * @author Yuhan Zhou 
     */
    public int getArrivingTime() {
        return arrivingTime;
    }
}
