import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

          ConcurrentHashMap<String, Set<ClientState>> watchRegistry = new ConcurrentHashMap<>();
          final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true);

          CommandHandler commandHandler = new CommandHandler(mp, expirationMap, listsMap, streamMap, globalLock, watchRegistry);

            while(true) {
                // Wait for connection from client.
                clientSocket = serverSocket.accept();
                final ClientState clientState = new ClientState();

                final Socket finalClientSocket1 = clientSocket;
                new Thread(()->{
                    try(Socket finalClientSocket = finalClientSocket1;
                        InputStream inputStream = finalClientSocket.getInputStream();
                        OutputStream outputStream = finalClientSocket.getOutputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    ) {
                        String line;
                        //final ClientState finalClientState = clientState;
                        Deque<String[]> transactionQueue = new ArrayDeque<>();

                        while((line=reader.readLine())!=null)
                        {
                            String output = "";
                            if (line.startsWith("*")) {
                                int length = Integer.parseInt(line.substring(1));
                                String[] inp = Resp.decodeBulkString(reader, length);

                                if ("MULTI".equalsIgnoreCase(inp[0])) {
                                    clientState.setTransactionStatus(TransactionStatus.PRE);
                                    output = Resp.encodeSimpleString("OK");
                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                } else if ("EXEC".equalsIgnoreCase(inp[0])) {
                                    if (clientState.getTransactionStatus() != TransactionStatus.PRE) {
                                        output = Resp.encodeError(Constants.EXEC_WITHOUT_MULTI_ERROR);
                                        outputStream.write(output.getBytes());
                                        outputStream.flush();
                                    } else {
                                        clientState.setTransactionStatus(TransactionStatus.EXECUTING);
                                        List<String> results = new ArrayList<>();

                                        globalLock.writeLock().lock();
                                        try {
                                            if(clientState.isDirty())
                                            {
                                                transactionQueue.clear();
                                                clientState.setTransactionStatus(TransactionStatus.NO);

                                                output = Resp.encodeArray(null);
                                                outputStream.write(output.getBytes());
                                                outputStream.flush();

                                                continue;
                                            }

                                            while (!transactionQueue.isEmpty()) {
                                                String result = commandHandler.handle(transactionQueue.removeFirst());
                                                results.add(result);
                                            }
                                        }finally {
                                            globalLock.writeLock().unlock();
                                        }
                                        clientState.setTransactionStatus(TransactionStatus.NO);
                                        output = Resp.joinAsRespArray(results);
                                        outputStream.write(output.getBytes());
                                        outputStream.flush();
                                    }

                                } else if ("DISCARD".equalsIgnoreCase(inp[0]))
                                {
                                    if(clientState.getTransactionStatus()!=TransactionStatus.PRE)
                                        output=Resp.encodeError(Constants.DISCARD_WITHOUT_MULTI_ERROR);
                                    else
                                    {
                                        transactionQueue.clear();
                                        clientState.setTransactionStatus(TransactionStatus.NO);
                                        output=Resp.encodeSimpleString("OK");
                                    }
                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                }
                                else if("WATCH".equalsIgnoreCase(inp[0]))
                                {
                                    if(clientState.getTransactionStatus()!=TransactionStatus.NO)
                                        output=Resp.encodeError(Constants.WATCH_INSIDE_MULTI_ERROR);
                                    else {
                                        for(int i=1; i<inp.length; i++)
                                        {
                                            String key = inp[1];
                                            watchRegistry.computeIfAbsent(key, k-> ConcurrentHashMap.newKeySet()
                                            ).add(clientState);
                                        }
                                        output = Resp.encodeSimpleString("OK");
                                    }

                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                }
                                else
                                {
                                    if(clientState.getTransactionStatus() == TransactionStatus.PRE)
                                    {
                                        transactionQueue.addLast(inp);
                                        output=Resp.encodeSimpleString("QUEUED");
                                        outputStream.write(output.getBytes());
                                        outputStream.flush();
                                    }
                                    else
                                    {
                                        output= commandHandler.handle(inp);
                                        outputStream.write(output.getBytes());
                                        outputStream.flush();
                                    }
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
