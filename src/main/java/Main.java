import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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

                                if ("PING".equals(inp[0])) {
                                    output = "+PONG\r\n";
                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                } else if ("ECHO".equals(inp[0])) {
                                    output = inp[1];
                                    String encodeBulkString = Resp.encodeBulkString(output);
                                    outputStream.write(encodeBulkString.getBytes());
                                    outputStream.flush();
                                }
                                else if("SET".equals(inp[0]))
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
                                else if("GET".equals(inp[0]))
                                {
                                    if(expirationMap.containsKey(inp[1]) && expirationMap.get(inp[1])<System.currentTimeMillis())
                                    {
                                        expirationMap.remove(inp[1]);
                                        mp.remove(inp[1]);
                                    }
                                    output = mp.getOrDefault(inp[1], null);
                                    String encodeBulkString = Resp.encodeBulkString(output);
                                    outputStream.write(encodeBulkString.getBytes());
                                    outputStream.flush();
                                }
                                else if("RPUSH".equals(inp[0]))
                                {
                                    List<String> list = listsMap.computeIfAbsent(inp[1], key->new CopyOnWriteArrayList<String>());
                                    for(int i=2; i<inp.length; i++)
                                        list.add(inp[i]);
                                    int size = list.size();
                                    String encodedInteger = Resp.encodeInteger(size);
                                    outputStream.write(encodedInteger.getBytes());
                                    outputStream.flush();
                                }
                                else if("LRANGE".equals(inp[0]))
                                {
                                    int l=Integer.parseInt(inp[2]);
                                    int r=Integer.parseInt(inp[3]);
                                    List<String> list = listsMap.getOrDefault(inp[1], null);
                                    List<String> rangeList = new ArrayList<>();
                                    if(list !=null)
                                    {
                                        for(int i=l; i<=Math.min(list.size()-1,r); i++)
                                            rangeList.add(list.get(i));
                                    }
                                    String encodedArray = Resp.encodeArray(rangeList);
                                    outputStream.write(encodedArray.getBytes());
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
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
