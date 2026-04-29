import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CommandHandler {
    private final ConcurrentHashMap<String,String> mp;
    private final ConcurrentHashMap<String, Long> expirationMap;
    private final ConcurrentHashMap<String, List<String>> listsMap;
    private final ConcurrentHashMap<String, Stream> streamMap;

    public CommandHandler(ConcurrentHashMap<String, String> mp, ConcurrentHashMap<String, Long> expirationMap, ConcurrentHashMap<String, List<String>> listsMap, ConcurrentHashMap<String, Stream> streamMap) {
        this.mp = mp;
        this.expirationMap = expirationMap;
        this.listsMap = listsMap;
        this.streamMap = streamMap;
    }

    public String handle(String[] inp)
    {
        String command = inp[0].toUpperCase();
        String output;
        List<String> list;
        String streamKey;
        String start;
        String end;
        String key;
        Stream stream;
        ConcurrentNavigableMap<StreamId, Map<String, String>> subEntries;
        try{
            switch (command){
                case "PING":
                    output = "PONG";
                    return Resp.encodeSimpleString(output);

                case "ECHO":
                    output = inp[1];
                    return Resp.encodeBulkString(output);

                case "SET":
                    mp.put(inp[1], inp[2]);
                    if(inp.length == 5 && "PX".equals(inp[3]))
                        expirationMap.put(inp[1],System.currentTimeMillis()+Long.parseLong(inp[4]));
                    else
                        expirationMap.remove(inp[1]);

                    output="OK";
                    return Resp.encodeSimpleString(output);

                case "GET":
                    Helper.removeExpiredFromKVMap(inp[1],mp, expirationMap);

                    output = mp.getOrDefault(inp[1], null);
                    return Resp.encodeBulkString(output);

                case "RPUSH":
                    list = listsMap.computeIfAbsent(inp[1], k->new CopyOnWriteArrayList<String>());
                    synchronized (list)
                    {
                        for(int i=2; i<inp.length; i++)
                            list.add(inp[i]);
                        list.notifyAll();
                    }
                    return Resp.encodeInteger(list.size());

                case "LRANGE":
                    list = listsMap.getOrDefault(inp[1], null);
                    List<String> rangeList = new ArrayList<>();
                    if(list !=null)
                    {
                        int l=Integer.parseInt(inp[2]);
                        int r=Integer.parseInt(inp[3]);

                        int len=list.size();

                        l = l<0? len+l: l;
                        l=Math.max(0,l);
                        r = r<0? len+r: r;
                        r=Math.min(len-1,r);

                        for(int i=l; i<=Math.min(list.size()-1,r); i++)
                            rangeList.add(list.get(i));
                    }

                    return Resp.encodeArray(rangeList);

                case "LPUSH":
                    list = listsMap.computeIfAbsent(inp[1], k->new CopyOnWriteArrayList<String>());
                    synchronized (list) {
                        List<String> toAdd = new ArrayList<>();
                        for (int i = inp.length - 1; i > 1; i--)
                            toAdd.add(inp[i]);
                        list.addAll(0, toAdd);
                        list.notify();
                    }

                    return Resp.encodeInteger(list.size());

                case "LLEN":
                    list = listsMap.get(inp[1]);
                    int size = (list==null)?0:list.size();

                    return Resp.encodeInteger(size);

                case "LPOP":
                    if(inp.length==2)
                    {
                        list = listsMap.get(inp[1]);
                        String removed = null;
                        if(list!=null) {
                            synchronized (list) {
                                if (!list.isEmpty())
                                    removed = list.remove(0);
                            }
                        }
                        return Resp.encodeBulkString(removed);

                    }
                    else if(inp.length==3)
                    {
                        list = listsMap.get(inp[1]);
                        List<String> removedItems = new ArrayList<>();
                        int toRemove=Integer.parseInt(inp[2]);

                        synchronized (list){
                            toRemove = Math.min(toRemove, list.size());
                            for(int i=0; i<toRemove; i++)
                            {
                                removedItems.add(list.remove(0));
                            }
                        }

                        return Resp.encodeArray(removedItems);
                    }

                case "BLPOP":
                    key = inp[1];
                    float timeout=Float.parseFloat(inp[2]);
                    long endTime = System.currentTimeMillis() + (long)(timeout*1000);
                    list = listsMap.computeIfAbsent(key, k->new CopyOnWriteArrayList<>());
                    String removed = null;
                    List<String> removedArray = null;
                    synchronized (list)
                    {
                        while(list.isEmpty())
                        {
                            long remainingTime = endTime - System.currentTimeMillis();
                            if(timeout>0 && remainingTime<=0) break;
                            try {
                                if(timeout==0)
                                    list.wait();
                                else
                                    list.wait(remainingTime);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }

                        }
                        if(!list.isEmpty()) removed=list.remove(0);
                    }
                    if(removed!=null)
                    {
                        removedArray = Arrays.asList(key,removed);
                    }

                    return Resp.encodeArray(removedArray);

                case "TYPE":
                    Helper.removeExpiredFromKVMap(inp[1], mp, expirationMap);

                    String type = "none";
                    if(mp.containsKey(inp[1]))
                        type="string";
                    if(streamMap.containsKey(inp[1]))
                        type="stream";

                    return Resp.encodeSimpleString(type);

                case "XADD":
                    streamKey = inp[1];
                    stream = streamMap.computeIfAbsent(streamKey, k->new Stream());
                    Map<String, String> entryMap = new HashMap<>();
                    for(int i=3; i<inp.length; i+=2)
                    {
                        String k=inp[i];
                        String v=inp[i+1];
                        entryMap.put(k,v);
                    }
                    String encodedOutput  = null;
                    try {
                        StreamId streamId = stream.addEntry(inp[2], entryMap);
                        encodedOutput = Resp.encodeBulkString(streamId.getStreamIdAsString());
                    } catch (StreamException e) {
                        encodedOutput = Resp.encodeError(e.getMessage());
                    }

                    return encodedOutput;

                case "XRANGE":
                    streamKey = inp[1];
                    start=inp[2];
                    end=inp[3];
                    subEntries = null;
                    if(streamMap.containsKey(streamKey))
                    {
                        stream = streamMap.get(streamKey);
                        subEntries =stream.getRange(start,true,end);
                    }
                    List<String> innerArrays = Helper.convertSubMapToEncodedArray(subEntries);

                    return Resp.joinAsRespArray(innerArrays);

                case "XREAD":
                    if("STREAMS".equalsIgnoreCase(inp[1])) {
                        int numStreams = (inp.length - 2) >> 1;
                        List<String> xReadResponseArrays = new ArrayList<>();
                        for (int i = 0; i < numStreams; i++) {
                            streamKey = inp[2 + i];
                            start = inp[2 + i + numStreams];
                            end = "+";

                            subEntries = null;
                            if (streamMap.containsKey(streamKey)) {
                                stream = streamMap.get(streamKey);
                                subEntries = stream.getRange(start, false, end);
                            }

                            String encodedArray = Helper.getStreamRangeWithKeyArrayEncoded(streamKey, subEntries);
                            xReadResponseArrays.add(encodedArray);
                        }

                        return Resp.joinAsRespArray(xReadResponseArrays);

                    }
                    else if("BLOCK".equalsIgnoreCase(inp[1])){
                        Long timeOutInMilliSecs = Long.parseLong(inp[2]);
                        List<String> xReadResponseArrays = new ArrayList<>();
                        String xReadResponseArraysEncoded;

                        streamKey = inp[4];
                        start = inp[5];
                        end = "+";

                        stream = streamMap.computeIfAbsent(streamKey, k-> new Stream());
                        subEntries = stream.getRangeBlocking(start, false, end, timeOutInMilliSecs);;
                        if(!subEntries.isEmpty()) {
                            String encodedArray = Helper.getStreamRangeWithKeyArrayEncoded(streamKey, subEntries);
                            xReadResponseArrays.add(encodedArray);
                            xReadResponseArraysEncoded = Resp.joinAsRespArray(xReadResponseArrays);
                        }
                        else
                            xReadResponseArraysEncoded = Resp.encodeArray(null);

                        return xReadResponseArraysEncoded;
                    }

                case "INCR":
                    key=inp[1];
                    Helper.removeExpiredFromKVMap(key, mp, expirationMap);
                    long val;
                    try {
                        val = Long.parseLong(mp.getOrDefault(key,"0"));
                    }catch (NumberFormatException e)
                    {
                        return Resp.encodeError(Constants.INC_KEY_NOT_INTEGER_ERROR);
                    }

                    long newValue = val+1;
                    mp.put(key, Long.toString(newValue));

                    return Resp.encodeInteger(newValue);

                default:
                    return Resp.encodeError("ERR unknown command '" + command + "'");
            }
        }
        catch (Exception e) {
            return Resp.encodeError("ERR " + e.getMessage());
        }
    }
}
