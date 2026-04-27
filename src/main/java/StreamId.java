import java.util.Objects;

public class StreamId implements Comparable<StreamId>{
    private final long millisecondsTime;
    private final long sequenceNumber;

    public StreamId(long time, long sequenceNumber) {
        this.millisecondsTime = time;
        this.sequenceNumber = sequenceNumber;
    }

    public String getStreamIdAsString()
    {
        return millisecondsTime+Constants.STREAM_DELIMITER+ sequenceNumber;
    }

    public boolean isZero(){
        return this.millisecondsTime==0 && this.sequenceNumber==0;
    }
    @Override
    public int compareTo(StreamId o) {
        if(this.millisecondsTime==o.millisecondsTime)
            return Long.compare(this.sequenceNumber, o.sequenceNumber);
        return Long.compare(this.millisecondsTime, o.millisecondsTime);
    }

    public long getMillisecondsTime() {
        return millisecondsTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamId streamId = (StreamId) o;
        return millisecondsTime == streamId.millisecondsTime && sequenceNumber == streamId.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(millisecondsTime, sequenceNumber);
    }
}
