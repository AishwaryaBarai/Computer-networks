import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileOwner {
    private static long chunkSize = 102400;
    private static int noOfPeers = 5;
    private static String inputFile = "/Users/apple/Desktop/test.pdf";

    private List<Integer> chuckInfo = new ArrayList<>();
    private ServerSocket sSocket;
    private int threadIndex;
    private Path currentRelativePath = Paths.get("");
    private String currentDir = currentRelativePath.toAbsolutePath().toString() + "/";
    private String tempDir = currentDir + "temp/";

    public static void main(String[] args) {
        try {
            String host = "localhost";
            String in = "";

            int sPort = 8000;
            if (args.length < 2) {
                System.out.println("Command line arguments were not provided. Setting to default values: localhost,8000");
            } else {
                host = args[0];
                sPort = Integer.parseInt(args[1]);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("enter number of peers [" + noOfPeers + "]:");
            in = reader.readLine();
            if (!in.equals(""))
                noOfPeers = Integer.parseInt(in);
            else
                System.out.println("setting the default value:" + noOfPeers);

            System.out.println("enter chunk size [" + chunkSize + "]:");
            in = reader.readLine();
            if (!in.equals(""))
                chunkSize = Integer.parseInt(in);
            else
                System.out.println("setting the default value:" + chunkSize);

            System.out.println("enter the file path [" + inputFile + "]:");
            in = reader.readLine();
            if (!in.equals(""))
                inputFile = in;
            else
                System.out.println("setting the default value:" + inputFile);

            FileOwner fileOwner = new FileOwner();
            System.out.println("starting server in address:" + host + " port:" + sPort);
            fileOwner.run(host, sPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run(String host, int sPort) {
        initFileChunk();
        try {
            InetAddress addr = InetAddress.getByName(host);
            sSocket = new ServerSocket(sPort, 10, addr);
            while (true) {
                Thread.sleep(500);
                System.out.println("[server] waiting for new connection..");
                Socket skt = sSocket.accept();
                threadIndex++;
                System.out.println("[server] connection accepted-" + threadIndex);
                ChunkDistributor chunkDistributor = new ChunkDistributor(skt, threadIndex);
                chunkDistributor.start();
            }
        } catch (IOException ioException) {
            System.out.println("[server] Failed to start the server:" + ioException.getMessage());
        } catch (InterruptedException ie) {
            System.out.println("[server] Failed to start the server:" + ie.getMessage());
        } finally {
            try {
                sSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initFileChunk() {
        try {
            System.out.println("[server] starting chunck initialization..");
            File file = new File(inputFile);
            FileInputStream is = new FileInputStream(file);
            long fileSize = file.length();
            if (fileSize / chunkSize < 5){
                chunkSize = fileSize / 5;
                System.out.println("chunk_size is too large for the file_size. resetting to:"+chunkSize);
            }
            System.out.println("[server] file size: " + fileSize + " chunk size:" + chunkSize + " # of chunks:" + fileSize / chunkSize);
            byte[] chunk = new byte[(int) chunkSize];
            int chunkLen = 0;
            int index = 0;
            while ((chunkLen = is.read(chunk)) != -1) {
                if (!Files.exists(Paths.get(tempDir)))
                    Files.createDirectory(Paths.get(tempDir));
                Path path = Paths.get(tempDir + index);
                Files.write(path, chunk);
                chuckInfo.add(index);
                chunk = new byte[(int) chunkSize];  // flushing old chunk and creating new space
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[server] chuck initialization complete");
    }


    class ChunkDistributor extends Thread {
        Socket connection;
        int peerIndex;
        ObjectOutputStream out;
        ObjectInputStream in;

        ChunkDistributor(Socket connection, int peerIndex) {
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
                    System.out.println("[server] client-" + peerIndex + ": " + cmd);
                    switch (cmd) {
                        case "":
                            Thread.sleep(500);
                            break;
                        case "hello":
                            sendMessage("hello");
                            sendMessage("filename " + Paths.get(inputFile).getFileName().toString());
                            break;
                        case "get peerId":
                            sendMessage(String.valueOf(peerIndex));
                            break;
                        case "get chunkInfo":
                            sendObject(chuckInfo);
                            System.out.println("[server] client-" + peerIndex + " receive status:" + readMessage());
                            break;
                        case "get chunks":
                            System.out.println("[server] client-" + peerIndex + " requesting for chunk..");
                            sendMessage("sending chunks");
                            if (!readMessage().equals("ok")) {
                                System.out.println("[server] error while sending chunks");
                            }
                            int startingChunk;
                            int endingChunk;
                            startingChunk = (peerIndex - 1) * (chuckInfo.size() / noOfPeers);

                            if (peerIndex == noOfPeers)
                                endingChunk = (peerIndex * (chuckInfo.size() / noOfPeers) + (chuckInfo.size() % noOfPeers)) - 1;
                            else if (peerIndex < noOfPeers)
                                endingChunk = (peerIndex * (chuckInfo.size() / noOfPeers)) - 1;
                            else {
                                startingChunk = 0;
                                endingChunk = 0;
                            }

                            System.out.println("[server] sending chunks [" + startingChunk + "-" + endingChunk + "] to client-" + peerIndex);
                            for (int i = startingChunk; i <= endingChunk; i++) {
                                Thread.sleep(100);
                                System.out.println("[server] sending chunk: " + i + " to client-" + peerIndex);
                                sendMessage(String.valueOf(i));
                                Thread.sleep(100);
                                Path path = Paths.get(tempDir + i);
                                Files.copy(path, out);
                            }
                            sendMessage("complete");
                            if (readMessage().equals("success")) {
                                System.out.println("[server] disconnecting from client-" + threadIndex + "..");
                                return;
                            }
                            break;
                        default:
                            System.out.println("[server] unknown command " + cmd + " from client-" + peerIndex);
                    }
                }
            } catch (IOException e) {
                System.out.println("[server] connection error with client: " + e.getMessage() + ". disconnecting..");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void sendMessage(String msg) {
            try {
//                System.out.println("[server] server: " + msg);
                out.writeObject(msg);
                out.flush();
            } catch (Exception e) {
                System.out.println("[server] Exception occurred while sending meesage to the client" + peerIndex + ": " + e.getMessage());
            }
        }

        private void sendObject(Object o) {
            try {
                out.writeObject(o);
                out.flush();
            } catch (Exception e) {
                System.out.println("[server] Exception occurred while sending object to the client" + peerIndex + ": " + e.getMessage());
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
