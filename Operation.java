public class Operation {
    OperationType type; // BEGIN, BEGIN_READ_ONLY, COMMIT, FAIL, RECOVER, READ, WRITE, DUMP
    int transactionID;
    int variableID;
    int valueToWrite;
}
