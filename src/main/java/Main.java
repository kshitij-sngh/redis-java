import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          ConcurrentHashMap<String,String> mp = new ConcurrentHashMap<>();
          ConcurrentHashMap<String, Long> expirationMap = new ConcurrentHashMap<>();
          ConcurrentHashMap<String, List<String>> listsMap = new ConcurrentHashMap<>();
          ConcurrentHashMap<String, Stream> streamMap = new ConcurrentHashMap<>();
            while(true) {
                // Wait for connection from client.
                clientSocket = serverSocket.accept();

                final Socket finalClientSocket1 = clientSocket;
                new Thread(()->{
                    try(Socket finalClientSocket = finalClientSocket1;
                        InputStream inputStream = finalClientSocket.getInputStream();
                        OutputStream outputStream = finalClientSocket.getOutputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    ) {
                        String line;
                        while((line=reader.readLine())!=null)
                        {
                            String output = "";

                            if (line.startsWith("*")) {
                                int length = Integer.parseInt(line.substring(1));
                                String[] inp = Resp.decodeBulkString(reader, length);

                                if ("PING".equalsIgnoreCase(inp[0])) {
                                    output = "+PONG\r\n";
                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                } else if ("ECHO".equalsIgnoreCase(inp[0])) {
                                    output = inp[1];
                                    String encodeBulkString = Resp.encodeBulkString(output);
                                    outputStream.write(encodeBulkString.getBytes());
                                    outputStream.flush();
                                }
                                else if("SET".equalsIgnoreCase(inp[0]))
                                {
                                    mp.put(inp[1], inp[2]);
                                    if(inp.length == 5 && "PX".equals(inp[3]))
                                        expirationMap.put(inp[1],System.currentTimeMillis()+Long.parseLong(inp[4]));
                                    else
                                        expirationMap.remove(inp[1]);

                                    output="+OK\r\n";
                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                }
                                else if("GET".equalsIgnoreCase(inp[0]))
                                {
                                    Helper.removeExpiredFromKVMap(inp[1],mp, expirationMap);

                                    output = mp.getOrDefault(inp[1], null);
                                    String encodeBulkString = Resp.encodeBulkString(output);
                                    outputStream.write(encodeBulkString.getBytes());
                                    outputStream.flush();
                                }
                                else if("RPUSH".equalsIgnoreCase(inp[0]))
                                {
                                    List<String> list = listsMap.computeIfAbsent(inp[1], key->new CopyOnWriteArrayList<String>());
                                    synchronized (list)
                                    {
                                        for(int i=2; i<inp.length; i++)
                                            list.add(inp[i]);
                                        list.notify();
                                    }
                                    String encodedInteger = Resp.encodeInteger(list.size());
                                    outputStream.write(encodedInteger.getBytes());
                                    outputStream.flush();
                                }
                                else if("LRANGE".equalsIgnoreCase(inp[0]))
                                {
                                    List<String> list = listsMap.getOrDefault(inp[1], null);
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
                                    String encodedArray = Resp.encodeArray(rangeList);
                                    outputStream.write(encodedArray.getBytes());
                                    outputStream.flush();
                                }
                                else if("LPUSH".equalsIgnoreCase(inp[0]))
                                {
                                    List<String> list = listsMap.computeIfAbsent(inp[1], key->new CopyOnWriteArrayList<String>());
                                    synchronized (list) {
                                        List<String> toAdd = new ArrayList<>();
                                        for (int i = inp.length - 1; i > 1; i--)
                                            toAdd.add(inp[i]);
                                        list.addAll(0, toAdd);
                                        list.notify();
                                    }
                                    String encodedInteger = Resp.encodeInteger(list.size());
                                    outputStream.write(encodedInteger.getBytes());
                                    outputStream.flush();
                                }
                                else if ("LLEN".equalsIgnoreCase(inp[0]))
                                {
                                    List<String> list = listsMap.get(inp[1]);
                                    int size = (list==null)?0:list.size();
                                    String encodedInteger = Resp.encodeInteger(size);
                                    outputStream.write(encodedInteger.getBytes());
                                    outputStream.flush();
                                }
                                else if("LPOP".equalsIgnoreCase(inp[0]))
                                {
                                    if(inp.length==2)
                                    {
                                        List<String> list = listsMap.get(inp[1]);
                                        String removed = null;
                                        if(list!=null) {
                                            synchronized (list) {
                                                if (!list.isEmpty())
                                                    removed = list.remove(0);
                                            }
                                        }
                                        String encodedBulkString = Resp.encodeBulkString(removed);
                                        outputStream.write(encodedBulkString.getBytes());
                                        outputStream.flush();
                                    }
                                    else if(inp.length==3)
                                    {
                                        List<String> list = listsMap.get(inp[1]);
                                        List<String> removedItems = new ArrayList<>();
                                        int toRemove=Integer.parseInt(inp[2]);

                                        synchronized (list){
                                            toRemove = Math.min(toRemove, list.size());
                                            for(int i=0; i<toRemove; i++)
                                            {
                                                removedItems.add(list.remove(0));
                                            }
                                        }

                                        String encodedArray = Resp.encodeArray(removedItems);
                                        outputStream.write(encodedArray.getBytes());
                                        outputStream.flush();
                                    }
                                }
                                else if("BLPOP".equalsIgnoreCase(inp[0]))
                                {
                                    String key = inp[1];
                                    float timeout=Float.parseFloat(inp[2]);
                                    long endTime = System.currentTimeMillis() + (long)(timeout*1000);
                                    List<String> list = listsMap.computeIfAbsent(key, k->new CopyOnWriteArrayList<>());
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

                                    String encodedArray = Resp.encodeArray(removedArray);
                                    outputStream.write(encodedArray.getBytes());
                                    outputStream.flush();
                                }
                                else if("TYPE".equalsIgnoreCase(inp[0]))
                                {
                                    Helper.removeExpiredFromKVMap(inp[1], mp, expirationMap);

                                    String type = "none";
                                    if(mp.containsKey(inp[1]))
                                        type="string";
                                    if(streamMap.containsKey(inp[1]))
                                        type="stream";
                                    String encodedString = Resp.encodeSimpleString(type);
                                    outputStream.write(encodedString.getBytes());
                                    outputStream.flush();
                                }
                                else if("XADD".equalsIgnoreCase(inp[0]))
                                {
                                    String streamKey = inp[1];
                                    Stream stream = streamMap.computeIfAbsent(streamKey, k->new Stream());
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

                                    outputStream.write(encodedOutput.getBytes());
                                    outputStream.flush();
                                }
                                else if("XRANGE".equalsIgnoreCase(inp[0]))
                                {
                                    String streamKey = inp[1];
                                    String start=inp[2], end=inp[3];
                                    ConcurrentNavigableMap<StreamId, Map<String, String>> subEntries = null;
                                    if(streamMap.containsKey(streamKey))
                                    {
                                        Stream stream = streamMap.get(streamKey);
                                        subEntries =stream.getRange(start,true,end);
                                    }
                                    List<String> innerArrays = Helper.convertSubMapToEncodedArray(subEntries);
                                    String encodedArray = Resp.joinAsRespArray(innerArrays);
                                    outputStream.write(encodedArray.getBytes());
                                    outputStream.flush();

                                }
                                else if("XREAD".equalsIgnoreCase(inp[0]))
                                {
                                    if("STREAMS".equalsIgnoreCase(inp[1])) {
                                        int numStreams = (inp.length - 2) >> 1;
                                        List<String> xReadResponseArrays = new ArrayList<>();
                                        for (int i = 0; i < numStreams; i++) {
                                            String streamKey = inp[2 + i];
                                            String start = inp[2 + i + numStreams];
                                            String end = "+";

                                            ConcurrentNavigableMap<StreamId, Map<String, String>> subEntries = null;
                                            if (streamMap.containsKey(streamKey)) {
                                                Stream stream = streamMap.get(streamKey);
                                                subEntries = stream.getRange(start, false, end);
                                            }

                                            String encodedArray = Helper.getStreamRangeWithKeyArrayEncoded(streamKey, subEntries);
                                            xReadResponseArrays.add(encodedArray);
                                        }

                                        String xReadResponseArraysEncoded = Resp.joinAsRespArray(xReadResponseArrays);
                                        outputStream.write(xReadResponseArraysEncoded.getBytes());
                                        outputStream.flush();
                                    }
                                    else if("BLOCK".equalsIgnoreCase(inp[1])){
                                        Long timeOutInMilliSecs = Long.parseLong(inp[2]);
                                        List<String> xReadResponseArrays = new ArrayList<>();
                                        String xReadResponseArraysEncoded;

                                        String streamKey = inp[4];
                                        String start = inp[5];
                                        String end = "+";

                                        Stream stream = streamMap.computeIfAbsent(streamKey, k-> new Stream());
                                        ConcurrentNavigableMap<StreamId, Map<String, String>> subEntries = stream.getRangeBlocking(start, false, end, timeOutInMilliSecs);;
                                        if(!subEntries.isEmpty()) {
                                            String encodedArray = Helper.getStreamRangeWithKeyArrayEncoded(streamKey, subEntries);
                                            xReadResponseArrays.add(encodedArray);
                                            xReadResponseArraysEncoded = Resp.joinAsRespArray(xReadResponseArrays);
                                        }
                                        else
                                            xReadResponseArraysEncoded = Resp.encodeArray(null);

                                        outputStream.write(xReadResponseArraysEncoded.getBytes());
                                        outputStream.flush();
                                    }

                                }
                                else if("INCR".equalsIgnoreCase(inp[0]))
                                {
                                    String key=inp[1];
                                    Helper.removeExpiredFromKVMap(key, mp, expirationMap);
                                    long newValue = Long.parseLong(mp.getOrDefault(key,"0"))+1;
                                    mp.put(key, Long.toString(newValue));

                                    String encodedInteger = Resp.encodeInteger(newValue);

                                    outputStream.write(encodedInteger.getBytes());
                                    outputStream.flush();
                                }
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        System.out.println("IOException: " + e.getMessage());
                    }
                }).start();
            }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }
}
