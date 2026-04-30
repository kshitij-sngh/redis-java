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

    Map<String, String> cmdLineArgsMap = Helper.createCmdLineMap(args);
    ServerState serverState = new ServerState();
    if(cmdLineArgsMap.containsKey("port"))
        try{
            int port = Integer.parseInt(cmdLineArgsMap.get("port"));
            serverState.setPort(port);
        }catch (NumberFormatException e)
        {
            System.out.println("Invalid port argument value passed, using default");
        }

    if(cmdLineArgsMap.containsKey("replicaof"))
    {
        serverState.setReplicationRole("slave");
        String[] parts =cmdLineArgsMap.get("replicaof").split(" ");
        serverState.setMasterHost(parts[0]);
        serverState.setMasterPort(Integer.parseInt(parts[1]));
    }



    if(serverState.isMaster())
    {
        serverState.setMasterReplicationId("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        serverState.setMasterReplicationOffset(0);
    }


    try {
        serverSocket = new ServerSocket(serverState.getPort());
        // Since the tester restarts your program quite often, setting SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        serverSocket.setReuseAddress(true);

        ConcurrentHashMap<String,String> mp = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> expirationMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, List<String>> listsMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Stream> streamMap = new ConcurrentHashMap<>();

        ConcurrentHashMap<String, Set<ClientState>> watchRegistry = new ConcurrentHashMap<>();
        final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true);

        CommandHandler commandHandler = new CommandHandler(mp, expirationMap, listsMap, streamMap, globalLock, watchRegistry, serverState);


        //Slave Handshake
        if(!serverState.isMaster())
        {
            new Thread(()->{
                //CODE here
                try {
                    String line;
                    Socket masterSocket;
                    OutputStream masterOutputStream;
                    InputStream masterInputStream;
                    BufferedReader masterInputStreamReader = null;

                    try {

                        masterSocket = new Socket(serverState.getMasterHost(), serverState.getMasterPort());
                        masterOutputStream = masterSocket.getOutputStream();
                        masterInputStream = masterSocket.getInputStream();
                        masterInputStreamReader = new BufferedReader(new InputStreamReader(masterInputStream));

                        Helper.sendCommandToMaster(masterOutputStream, List.of("PING"));
                        line = masterInputStreamReader.readLine();

                        Helper.sendCommandToMaster(masterOutputStream, List.of("REPLCONF", "listening-port", Integer.toString(serverState.getPort())));
                        line = masterInputStreamReader.readLine();

                        Helper.sendCommandToMaster(masterOutputStream, List.of("REPLCONF", "capa", "psync2"));
                        line = masterInputStreamReader.readLine();

                        Helper.sendCommandToMaster(masterOutputStream, List.of("PSYNC", "?", "-1"));
                        line = masterInputStreamReader.readLine();
                        //processing RDB file, draining RDB here
                        line = masterInputStreamReader.readLine();
                        if(line!=null && line.startsWith("$"))
                        {
                            int rdbLength = Integer.parseInt(line.substring(1));
                            System.out.println("Processing RDB of length: "+rdbLength);

                            long skipped = 0;
                            while(skipped<rdbLength)
                                skipped += masterInputStream.skip(rdbLength-skipped);

                            System.out.println("RDB processed successfully");
                        }
                    }catch (IOException e)
                    {
                        System.out.println("Handshake IOException: " + e.getMessage());
                    }

                    while ((line = masterInputStreamReader.readLine()) != null)
                    {
                        if (line.startsWith("*")) {
                            int length = Integer.parseInt(line.substring(1));
                            String[] inp = Resp.decodeBulkString(masterInputStreamReader, length);

                            String output= commandHandler.handle(inp);
                        }
                    }
                }catch (IOException e)
                {
                    System.out.println("Propagation IOException: " + e.getMessage());
                }
            }).start();
        }
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
                                        Helper.unwatchAll(clientState,watchRegistry);
                                        clientState.setTransactionStatus(TransactionStatus.NO);
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
                                    Helper.unwatchAll(clientState, watchRegistry);
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
                                        String key = inp[i];
                                        watchRegistry.computeIfAbsent(key, k-> ConcurrentHashMap.newKeySet()
                                        ).add(clientState);
                                        clientState.getWatchedKeys().add(key);
                                    }
                                    output = Resp.encodeSimpleString("OK");
                                }

                                outputStream.write(output.getBytes());
                                outputStream.flush();
                            }
                            else if("UNWATCH".equalsIgnoreCase(inp[0]))
                            {
                                Helper.unwatchAll(clientState, watchRegistry);

                                output=Resp.encodeSimpleString("OK");
                                outputStream.write(output.getBytes());
                                outputStream.flush();
                            }
                            else if("INFO".equalsIgnoreCase(inp[0]))
                            {
                                if(inp.length>=2 && "replication".equalsIgnoreCase(inp[1]))
                                {
                                    output = Helper.getServerReplicationInfo(serverState);
                                    output = Resp.encodeBulkString(output);

                                    outputStream.write(output.getBytes());
                                    outputStream.flush();
                                }
                            }
                            else if("REPLCONF".equalsIgnoreCase(inp[0]))
                            {
                                output=Resp.encodeSimpleString("OK");
                                outputStream.write(output.getBytes());
                                outputStream.flush();
                            }
                            else if("PSYNC".equalsIgnoreCase(inp[0]))
                            {
                                output=Resp.encodeSimpleString("FULLRESYNC "+serverState.getMasterReplicationId()+" "+serverState.getMasterReplicationOffset());
                                outputStream.write(output.getBytes());
                                outputStream.flush();

                                byte[] fileContentsDecodedBytes = Base64.getDecoder().decode(Constants.EMPTY_RDB_FILE_ENCODED);

                                output="$"+fileContentsDecodedBytes.length+"\r\n";
                                outputStream.write(output.getBytes());
                                outputStream.write(fileContentsDecodedBytes);
                                outputStream.flush();

                                serverState.masterReplicationSlavesAddSlave(outputStream);
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
                finally {
                    Helper.unwatchAll(clientState, watchRegistry);
                    System.out.println("Client disconnected and watch registry cleaned up.");
                }
            }).start();
        }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
