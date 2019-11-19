import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Server {
    //    static int numOfChuncks = 5;
    static long chunkSize = 100000;
    static int noOfPeers = 5;
    Map<Integer, byte[]> map = new HashMap<>();
    Map<Integer, Integer> chuckInfo = new HashMap<>();
    ServerSocket sSocket;
    int threadIndex;
    long fileSize;
    Path currentRelativePath = Paths.get("");
    String currentDir = currentRelativePath.toAbsolutePath().toString() + "/";
    String inputFile="/Users/apple/Desktop/test.pdf";

    public static void main(String[] args) {
        String host = "localhost";
        int sPort = 8000;
        if (args.length < 2) {
            System.out.println("Command line arguements were not provided. Setting to default values: localhost,8000");
        } else {
            host = args[0];
            sPort = Integer.parseInt(args[1]);
        }
        Server server = new Server();
        System.out.println("starting server in address:" + host + " port:" + sPort);
        server.run(host, sPort);
    }

    void run(String host, int sPort) {
        initFileChunk();
        try {
            InetAddress addr = InetAddress.getByName(host);
            sSocket = new ServerSocket(sPort, 10, addr);
            while (true) {
                Thread.sleep(500);
                System.out.println("waiting for new connection..");
                Socket skt = sSocket.accept();
                threadIndex++;
                System.out.println("connection accepted!");
                ChunkDistributor chunkDistributor = new ChunkDistributor(skt, threadIndex);
                chunkDistributor.start();
            }
        } catch (IOException ioException) {
            System.out.println("Failed to start the server:" + ioException.getMessage());
        } catch (InterruptedException ie) {
            System.out.println("Failed to start the server:" + ie.getMessage());
        } finally {
            try {
                sSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void initFileChunk() {
        try {
            System.out.println("starting chuck initialization..");
            File file = new File(inputFile);
            FileInputStream is = new FileInputStream(file);
            fileSize = file.length();
            System.out.println("file size: " + fileSize + " chunk size:" + chunkSize+" # of chunks:"+fileSize/chunkSize);
            byte[] chunk = new byte[(int) chunkSize];  //todo cast to int
            int chunkLen = 0;
            int index = 0;
            while ((chunkLen = is.read(chunk)) != -1) {
                map.put(index, chunk);
                chuckInfo.put(index, 1);
                chunk=new byte[(int) chunkSize];
                index++;
            }
        } catch (FileNotFoundException fnfE) {
            // file not found, handle case
        } catch (IOException ioE) {
            // problem reading, handle case
        }
        System.out.println("chuck initialization complete");
    }


    class ChunkDistributor extends Thread {
        Socket connection;   //serversocket used to listen on port number 8000
        int peerIndex;
        ObjectOutputStream out;  //stream write to the socket
        ObjectInputStream in;    //stream read from the socket
        boolean isAuthenticated;    //stream read from the socket

        public ChunkDistributor(Socket connection, int peerIndex) {
            this.connection = connection;
            this.peerIndex = peerIndex;
        }

        public void run() {
            try {
                out = new ObjectOutputStream(connection.getOutputStream());
                out.flush();
                in = new ObjectInputStream(connection.getInputStream());
                while (true) {
                    String cmd = readMessage();
                    System.out.println("client-" + peerIndex + ": " + cmd);
                    switch (cmd) {
                        case "":
                            Thread.sleep(500);
                            break;
                        case "hello":
                            sendMessage("hello");
                            break;
                        case "get peerId":
                            sendMessage(String.valueOf(peerIndex));
                            break;
                        case "get chunkInfo":
                            sendObject(chuckInfo);
                            System.out.println("client-" + peerIndex + " receive status:" + readMessage());
                            break;
                        case "get chunks":
                            System.out.println("client-" + peerIndex + " requesting for chunk..");
                            sendMessage("sending chunks");
                            if (!readMessage().equals("ok")) {
                                System.out.println("error while sending chunks");
                            }
                            int startingChunk;
                            int endingChunk;
                            startingChunk = (peerIndex - 1) * (map.size() / noOfPeers);

                            if (peerIndex == noOfPeers)
                                endingChunk = (peerIndex * (map.size() / noOfPeers) + (map.size() % noOfPeers)) - 1;
                            else
                                endingChunk = (peerIndex * (map.size() / noOfPeers)) - 1;

//                            startingChunk=0;
//                            endingChunk=map.size()-1;

                            System.out.println("sending chunks [" + startingChunk+ "-" +endingChunk + "]");
                            for (int i = startingChunk; i <= endingChunk; i++) {
                                Thread.sleep(1000);
                                System.out.println("sending chunk: " + i);
                                sendMessage(String.valueOf(i));
                                Thread.sleep(100);
                                out.write(map.get(i));
                                out.flush();
                            }
                            sendMessage("complete");
                            if (readMessage().equals("success")) {
                                System.out.println("disconnecting from client-" + threadIndex + "..");
                                return;
                            }
                            break;
                        default:
                            System.out.println("unknown command " + cmd + " from client-" + peerIndex);
                    }
                }
            } catch (IOException e) {
                System.out.println("connection error with client: " + e.getMessage() + ". disconnecting..");
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void sendMessage(String msg) {
            try {
                System.out.println("server: " + msg);
                out.writeObject(msg);
                out.flush();
            } catch (Exception e) {
                System.out.println("Exception occurred while sending meesage to the client" + peerIndex + ": " + e.getMessage());
            }
        }

        private void sendObject(Object o) {
            try {
                out.writeObject(o);
                out.flush();
            } catch (Exception e) {
                System.out.println("Exception occurred while sending object to the client" + peerIndex + ": " + e.getMessage());
            }
        }

        private String readMessage() throws Exception {
            String str = "";
            try {
                str = (String) in.readObject();
            } catch (ClassNotFoundException ce) {
                System.err.println("Unable to parse message from the client:" + peerIndex + ": " + ce.getMessage());
            } catch (SocketException e) {
                System.err.println("Connection error with client-" + peerIndex + ": " + e.getMessage());
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Unable to read message from the client:" + peerIndex + ": ");
            }
            return str;
        }
    }
}
