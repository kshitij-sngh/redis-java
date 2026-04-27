import java.util.HashMap;
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
        return id;
    }
    private StreamId parseInputId(String inputId)
    {
        String[] parts = inputId.split(Constants.STREAM_DELIMITER);
        long time = Long.parseLong(parts[0]);
        long seq;
        if("*".equals(parts[1]))
        {
            if(entries.isEmpty())
                return new StreamId(0,1);
            else
            {
                StreamId lastId = entries.lastKey();
                if(lastId.getMillisecondsTime() == time){
                    return new StreamId(time, lastId.getSequenceNumber()+1);
                }
                else if(time==0)
                    return new StreamId(time, 1);
                else
                    return new StreamId(time, 0);
            }
        }
        else seq = Long.parseLong(parts[1]);

        return new StreamId(time, seq);
    }
}
