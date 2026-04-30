import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class Helper {
    public static List<String> convertSubMapToEncodedArray(ConcurrentNavigableMap<StreamId, Map<String,String>> subEntries)
    {
        List<String> innerArrays = new ArrayList<>();
        if(subEntries!=null) {
            for (Map.Entry<StreamId, Map<String, String>> e : subEntries.entrySet()) {
                StreamId streamId = e.getKey();
                Map<String, String> kvMap = e.getValue();
                List<String> kvList = new ArrayList<>();
                for (Map.Entry<String, String> eKV : kvMap.entrySet()) {
                    kvList.add(eKV.getKey());
                    kvList.add(eKV.getValue());
                }
                String streamIdEncodedBulkString = Resp.encodeBulkString(streamId.getStreamIdAsString());
                String kvEncodedArray = Resp.encodeArray(kvList);


                innerArrays.add(Resp.joinAsRespArray(List.of(streamIdEncodedBulkString, kvEncodedArray)));
            }
        }
        return innerArrays;
    }
    public static String getStreamRangeWithKeyArrayEncoded(String streamKey, ConcurrentNavigableMap<StreamId, Map<String, String>> subEntries){
        String streamKeyBulk = Resp.encodeBulkString(streamKey);
        List<String> innerArrays = Helper.convertSubMapToEncodedArray(subEntries);
        String encodedInnerArrays = Resp.joinAsRespArray(innerArrays);
        List<String> outerArray = List.of(streamKeyBulk,encodedInnerArrays);
        String encodedArray = Resp.joinAsRespArray(outerArray);
        return encodedArray;
    }
    public static void removeExpiredFromKVMap(String key, ConcurrentHashMap<String,String> mp,
                            ConcurrentHashMap<String, Long> expirationMap)
    {
        if(expirationMap.containsKey(key) && expirationMap.get(key)<System.currentTimeMillis())
        {
            expirationMap.remove(key);
            mp.remove(key);
        }
    }

    public static void unwatchAll(ClientState clientState, ConcurrentHashMap<String, Set<ClientState>> watchRegistry)
    {
        List<String> keys=clientState.getWatchedKeys();
        for(String key: keys)
            watchRegistry.get(key).remove(clientState);

        keys.clear();
        clientState.setDirty(false);
    }

    public static Map<String, String> createCmdLineMap(String[] args)
    {
        Map<String, String> cmdLineArgsMap = new HashMap<>();
        for(int i=0; i<args.length; i+=2)
            if(args[i].startsWith("--")&& i+1<args.length) {
                cmdLineArgsMap.put(args[i].substring(2),args[i+1]);
            }
        return cmdLineArgsMap;
    }
    public static String getServerReplicationInfo(ServerState serverState)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("# Replication\r\nrole:");
        sb.append(serverState.getReplicationRole());
        sb.append("\r\n");
        if(serverState.isMaster())
        {
            sb.append("mater_replid:");
            sb.append(serverState.getMasterReplicationId());
            sb.append("\r\n");
            sb.append("master_repl_offset:");
            sb.append(serverState.getMasterReplicationOffset());
        }

        return sb.toString();
    }
}
