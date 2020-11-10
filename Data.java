public class Data {
    int variableID;
    DataType type;
    boolean written;    // false after failure, true after written again
    boolean available;  // false after failure, true after written and committed again
    int currentValue;  // might be uncommited value
    int commitedValue;    // only if the transaction commits, then this commitedValue is set to currentValue
}
