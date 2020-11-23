/**
 * information about the data copy stored on the data manager
 */

class DataCopy {
    private int currentValue;
    private int committedValue;
    private boolean readAvailable;
    private DataType dataType;


    public DataCopy(DataType dataType, int initialValue)
    {
        this.currentValue = initialValue;
        this.committedValue = initialValue;
        this.readAvailable = true;
        this.dataType = dataType;
    }

    public int getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(int currentValue) {
        this.currentValue = currentValue;
    }

    public int getCommittedValue() {
        return committedValue;
    }

    public void setCommittedValue(int committedValue) {
        this.committedValue = committedValue;
    }

    public boolean isReadAvailable() {
        return readAvailable;
    }

    public void setReadAvailable(boolean readAvailable) {
        this.readAvailable = readAvailable;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }
}