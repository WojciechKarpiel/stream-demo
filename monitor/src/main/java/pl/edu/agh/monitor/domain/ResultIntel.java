package pl.edu.agh.monitor.domain;

public class ResultIntel {

    private long timestamp;
    private long receivedMessagesCount;

    ResultIntel() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getReceivedMessagesCount() {
        return receivedMessagesCount;
    }

    public void setReceivedMessagesCount(long receivedMessagesCount) {
        this.receivedMessagesCount = receivedMessagesCount;
    }

}
