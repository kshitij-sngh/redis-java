import java.util.Objects;

public class StreamId implements Comparable<StreamId>{
    private final long time;
    private final long sequence;

    public StreamId(long time, long sequence) {
        this.time = time;
        this.sequence = sequence;
    }

    @Override
    public int compareTo(StreamId o) {
        if(this.time==o.time)
            return Long.compare(this.sequence, o.sequence);
        return Long.compare(this.time, o.time);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamId streamId = (StreamId) o;
        return time == streamId.time && sequence == streamId.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, sequence);
    }
}
