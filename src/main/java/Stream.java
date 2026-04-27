import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Stream {
    private final ConcurrentNavigableMap<StreamId, Map<String, String>> entries = new ConcurrentSkipListMap<>();
    public synchronized void addEntry(StreamId id, Map<String, String> values)
    {
        entries.put(id, values);
    }
}
