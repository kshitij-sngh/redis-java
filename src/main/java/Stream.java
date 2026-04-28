import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Stream {
    private final ConcurrentNavigableMap<StreamId, Map<String, String>> entries = new ConcurrentSkipListMap<>();
    public synchronized StreamId addEntry(String providedStreamId, Map<String, String> values) throws StreamException {
        StreamId id = parseInputId(providedStreamId);

        if(id.isZero())
            throw new StreamException(Constants.STREAM_ADD_ZERO_ERROR);
        if(!entries.isEmpty())
        {
            StreamId lastId = entries.lastKey();
            if(id.compareTo(lastId)<=0)
                throw new StreamException(Constants.STREAM_ADD_LESS_THAN_LAST_ERROR);
        }

        entries.put(id, values);
        notifyAll();
        return id;
    }

    public ConcurrentNavigableMap<StreamId, Map<String, String>> getRange(String start, boolean startInclusive, String end) {
        if (entries.isEmpty())
            return new ConcurrentSkipListMap<>();

        StreamId startId = "-".equals(start) ? entries.firstKey() : parseInputId(start);
        StreamId endId = "+".equals(end) ? entries.lastKey() : parseInputId(end);

        if (entries.isEmpty()) return new ConcurrentSkipListMap<>();
        return entries.subMap(startId, startInclusive, endId, true);
    }

    public synchronized ConcurrentNavigableMap  <StreamId, Map<String, String>> getRangeBlocking(String start, boolean startInclusive, String end, Long timeOut) {
        StreamId startId;
        if("$".equals(start))
            startId = entries.isEmpty() ? new StreamId(0,0) : entries.lastKey();
        else
            startId = "-".equals(start) ? new StreamId(0,0) : parseInputId(start);

        StreamId endId = "+".equals(end) ? new StreamId(Long.MAX_VALUE, Long.MAX_VALUE) : parseInputId(end);

        long endTime = System.currentTimeMillis()+timeOut;

        ConcurrentNavigableMap<StreamId, Map<String, String>> results = entries.subMap(startId, startInclusive, endId, true);

        try {
            while (results.isEmpty()) {
                long remainingTime = endTime - System.currentTimeMillis();
                if (timeOut > 0 && remainingTime<=0) {
                        break;
                }
                if(timeOut==0)
                    wait();
                else
                    wait(remainingTime);
            }
        }
        catch (InterruptedException e){
            Thread.currentThread().interrupt();
            return results;
        }
        return results;
    }

    private long generateSequenceNumber(long millisecondsTime)
    {
        if(entries.isEmpty()) {
            if (millisecondsTime == 0)
                return 1;
            return 0;
        }
        else
        {
            StreamId lastId = entries.lastKey();
            if(lastId.getMillisecondsTime() == millisecondsTime){
                return lastId.getSequenceNumber()+1;
            }
            else if(millisecondsTime==0)
                return 1;
            else
                return 0;
        }
    }
    private StreamId parseInputId(String inputId)
    {
        if("*".equals(inputId))
        {
            long time = System.currentTimeMillis();
            long seq = generateSequenceNumber(time);
            return new StreamId(time,seq);

        }
        String[] parts = inputId.split(Constants.STREAM_DELIMITER);
        long time = Long.parseLong(parts[0]);
        long seq;
        if("*".equals(parts[1]))
        {
            seq = generateSequenceNumber(time);
            return new StreamId(time, seq);
        }

        seq = Long.parseLong(parts[1]);
        return new StreamId(time, seq);
    }


}
