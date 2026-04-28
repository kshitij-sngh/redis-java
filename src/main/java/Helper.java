import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
}
