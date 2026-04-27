import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Stream {
    private final ConcurrentNavigableMap<StreamId, Map<String, String>> entries = new ConcurrentSkipListMap<>();
    public synchronized String addEntry(StreamId id, Map<String, String> values)
    {
        if(id.isZero())
            return Constants.STREAM_ADD_ZERO_ERROR;
        if(!entries.isEmpty())
        {
            StreamId lastId = entries.lastKey();
            if(id.compareTo(lastId)<=0)
                return Constants.STREAM_ADD_LESS_THAN_LAST_ERROR;
        }
        entries.put(id, values);
        return null;
    }
}
