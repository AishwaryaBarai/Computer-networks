import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class Peer {
    private static String logPrefix;
    private Map<Integer, Integer> chuckInfo = new HashMap<>();
    private Path currentRelativePath = Paths.get("");
    private String currentDir = currentRelativePath.toAbsolutePath().toString() + "/";
    private String tempDir = currentDir + "temp/";
    private int pubPort = 2222;
    private int fileOwnerPort = 8000;
    private String host = "localhost";
    private String peerId;
    private List<String> needChunks = Collections.synchronizedList(new ArrayList<>());
    private Map<String, Boolean> haveChunks = new HashMap<>();
    private List<String> knownPeers = Collections.synchronizedList(new ArrayList<>());
    private String fileName = "test.pdf";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Command line arguments were not provided");
            return;
        }
        Peer peer = new Peer();
        logPrefix = "[" + args[1] + "] ";
        peer.run(args[0], args[1], args[2]);
    }

    private void run(String fOPort, String publishPort, String knownPeer) {
        String input;
        String logPrefix2 = logPrefix + " [init]";
        try {
            if (!Files.exists(Paths.get(tempDir)))
                Files.createDirectory(Paths.get(tempDir));
            pubPort = Integer.parseInt(publishPort);
            knownPeers.add(knownPeer);
            fileOwnerPort = Integer.parseInt(fOPort);
            Socket requestSocket = new Socket(host, fileOwnerPort);
            SocketUtil socketUtil = new SocketUtil(requestSocket);

            socketUtil.sendMessage("hello");
            if (socketUtil.readMessage().equals("hello")) {
                String str = socketUtil.readMessage();
                if (str.split("filename").length < 2) {
                    System.err.println(logPrefix2 + "connection error with server; please restart the peer and try again");
                    return;
                }
                fileName = str.split("filename")[1];
            } else {
                System.err.println(logPrefix2 + "connection error with server; please restart the peer and try again");
            }
            socketUtil.sendMessage("get peerId");
            peerId = socketUtil.readMessage();

            System.out.println(logPrefix2 + "requesting chunk info..");
            try {
                socketUtil.sendMessage("get chunkInfo");
                chuckInfo = (Map) socketUtil.getIn().readObject();
                socketUtil.sendMessage("success");
            } catch (Exception e) {
                System.out.println(logPrefix2 + "unable to parse chuck info" + e.getMessage());
                System.exit(0);
            }
            for (int chunkId : chuckInfo.keySet()) {
                needChunks.add(String.valueOf(chunkId));
            }
            System.out.println(logPrefix2 + "chunk info fetched successfully! " + chuckInfo.get(1));
            System.out.println(logPrefix2 + "requesting chunks from the server..");
            try {
                socketUtil.sendMessage("get chunks");
                if (!socketUtil.readMessage().equals("sending chunks")) {
                    System.out.println(logPrefix2 + "error while receiving chunks from server");
                    return;
                }
                socketUtil.sendMessage("ok");
                String str = socketUtil.readMessage();
                System.out.println(logPrefix2 + "startin to receive chunk " + str);
                while (str.equals("") || !str.equals("complete")) {
                    System.out.println(logPrefix2 + "fetching chunk:" + str + " from server..");
                    Path path = Paths.get(tempDir + str);
                    Files.copy(socketUtil.getIn(), path, StandardCopyOption.REPLACE_EXISTING);
                    //todo checksum validation
                    needChunks.remove(str);
                    haveChunks.put(str, false);
                    str = socketUtil.readMessage();
                }
                socketUtil.sendMessage("success");
                System.out.println(logPrefix2 + "chunks retrieved from the server successfully");
                System.out.println(logPrefix2 + "disconnecting from server..");
                socketUtil.close();
                Thread t1 = new Thread(() -> {
                    receiveChunks();
                    System.out.println("stopped receiving chunks");
                });
                t1.start();
                publishChunks();
                System.out.println("stopped publishing chunks..");
            } catch (Exception e) {
                System.out.println("error while receiving file from the server: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (ConnectException e) {
            System.err.println("Connection refused. You need to initiate a server first.");
            System.exit(0);

        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
            System.exit(0);
        } catch (IOException ioException) {
            System.err.println("Cannot connect with the server: " + ioException.getMessage());
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void receiveChunks() {
        String logPrefix2 = logPrefix + " [chunk_receiver] ";
        System.out.println("starting to receive chunks from other peers..");
        try {
            Socket socket = null;
            SocketUtil socketUtil;
            boolean flag = true;
            while (flag) {
                List<String> copyOfKnownPeers = new ArrayList<>(knownPeers);
                for (String portStr : copyOfKnownPeers) {
                    flag = false;
                    try {
                        System.out.println(logPrefix2 + "trying to connect to peer:" + portStr);
                        socket = new Socket(host, Integer.parseInt(portStr));
                        System.out.println(logPrefix2 + "connected to peer:" + portStr + " successfully");
                        socketUtil = new SocketUtil(socket);
                        socketUtil.sendMessage("hello");
                        if (!socketUtil.readMessage().equals("hello")) {
                            System.out.println(logPrefix2 + "something wrong; disconnecting peer:" + portStr);
                            return;
                        }
                        socketUtil.sendMessage("i'm: " + peerId + " host " + host + " port " + pubPort);
                        if (!socketUtil.readMessage().equals("ok")) {
                            System.out.println("something wrong; disconnecting peer:" + portStr);
                            return;
                        }
                        List<String> chunks = new ArrayList<>();
                        for (String chunkId : needChunks) {
                            System.out.println(logPrefix2 + "asking peer:" + portStr + " about chunk:" + chunkId);
                            socketUtil.sendMessage("need " + chunkId);
                            String str = socketUtil.readMessage();
                            if (str.equals("i have")) {
                                System.out.println(logPrefix2 + "fetching chunk-" + chunkId + " from peer:" + portStr + "..");
                                Path path = Paths.get(tempDir + chunkId);
                                Files.copy(socketUtil.getIn(), path, StandardCopyOption.REPLACE_EXISTING);
                                System.out.println(logPrefix2 + "received chunk:" + chunkId + " successfully");
                                System.out.println(socketUtil.readMessage());
                                socketUtil.sendMessage("success");
                                chunks.add(chunkId);
                                Thread.sleep(1000);
                            } else {
                                System.out.println(logPrefix2 + "peer:" + portStr + " don't have the chunk:" + chunkId + " yet!");
                                Thread.sleep(1000);
                            }
                        }
                        for (String chunkId : chunks) {
                            System.out.println(logPrefix2 + "updating status for chunk..........................." + chunkId);
                            needChunks.remove(chunkId);
                            haveChunks.put(chunkId, false);
                        }
                        System.out.println(logPrefix2 + "disconnecting from peer: " + portStr);
                        socketUtil.sendMessage("bye");
                        if (!needChunks.isEmpty())
                            flag = true;
                        socketUtil.close();
                        Thread.sleep(2000);
                    } catch (Exception ioException) {
                        flag = true;
                        System.out.println(logPrefix2 + "failed connect to peer:" + portStr);
                        Thread.sleep(5000);
                    }
                }
            }
            System.out.println(logPrefix2 + "i have all chunk with me; combining chunks");
            FileOutputStream fos = new FileOutputStream(currentDir + fileName);
            for (Integer chunkId : chuckInfo.keySet()) {
                System.out.println("combining chunks......." + chunkId);
                fos.write(Files.readAllBytes(Paths.get(tempDir + chunkId)));
                fos.flush();
            }
            fos.close();
            System.out.println("***********************************************************");
            System.out.println("                 FINAL FILE IS READY :)");
            System.out.println("***********************************************************");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void publishChunks() {
        ServerSocket pubSocket;
        SocketUtil socketUtil;
        String logPrefix2 = logPrefix + " [chunk_publisher] ";
        try {
            InetAddress addr = InetAddress.getByName(host);
            pubSocket = new ServerSocket(pubPort, 10, addr);
            while (true) {
                System.out.println(logPrefix2 + "waiting for new peer requests..");
                socketUtil = new SocketUtil(pubSocket.accept());
                System.out.println(logPrefix2 + "connection accepted for a new peer");
                if (!socketUtil.readMessage().equals("hello")) {
                    System.out.println(logPrefix2 + "something wrong; disconnecting peer..");
                    return;
                }
                socketUtil.sendMessage("hello");
                String[] strs = socketUtil.readMessage().split(" ");
                if (strs.length < 6) {
                    System.out.println(logPrefix2 + "something wrong; disconnecting peer..");
                    return;
                }
                String peerPort=strs[5];
                if (!knownPeers.contains(peerPort)) {
                    System.out.println(logPrefix2 + "new peer identified port:" + peerPort);
                    knownPeers.add(peerPort);
                }
                socketUtil.sendMessage("ok");
                String req = socketUtil.readMessage();
                if (req.equals("bye")) {
                    System.out.println(logPrefix2 + "disconnecting connection with peer:" + peerPort);
                    socketUtil.close();
                }
                while (req.startsWith("need")) {
                    if (req.split(" ").length < 2)
                        continue;
                    else {
                        String chunkId=req.split(" ")[1];
                        System.out.println(logPrefix2 + "peer:" + peerPort + "  asking for the chunk " + chunkId);
                        if (haveChunks.containsKey(chunkId)) {
                            socketUtil.sendMessage("i have");
                            System.out.println(logPrefix2 + "sending chunk:" + chunkId + " to peer:" + peerPort);
                            Path path = Paths.get(tempDir + chunkId);
                            Files.copy(path, socketUtil.getOut());
                            if (!socketUtil.readMessage().equals("success")) {
                                System.out.println(logPrefix2 + "something wrong; disconnecting peer..");
                                return;
                            } else {
                                haveChunks.put(chunkId, true);
                            }
                            Thread.sleep(1000);
                        } else {
                            System.out.println(logPrefix2 + "i don't have chunk:" + chunkId);
                            socketUtil.sendMessage("don't have");
                        }
                    }
                    req = socketUtil.readMessage();
                }
                if (!haveChunks.containsValue(false)) {
                    System.out.println(logPrefix2 + "I have sent all chunks to atleast one other peer..");
                }
            }
        } catch (Exception e) {
            System.out.println(logPrefix2 + "Failed to start the server:" + e.getMessage());
        }
    }

    public class SocketUtil {
        private Socket socket;           //socket connect to the server
        private ObjectOutputStream out;         //stream write to the socket
        private ObjectInputStream in;          //stream read from the socket

        public SocketUtil(Socket socket) {
            this.socket = socket;
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        ObjectOutputStream getOut() {
            return out;
        }

        ObjectInputStream getIn() {
            return in;
        }

        void sendMessage(String msg) {
            try {
//                System.out.println("Me: " + msg);
                out.writeObject(msg);
                out.flush();
            } catch (Exception e) {
                System.out.println("Exception occurred while sending message:" + e.getMessage());
            }
        }

        void sendObject(Object o) {
            try {
                out.writeObject(o);
                out.flush();
            } catch (Exception e) {
                System.out.println("exception occurred while sending object" + e.getMessage());
            }
        }

        String readMessage() throws Exception {
            String str = "";
            try {
                str = (String) in.readObject();
//                System.out.println("remote: " + str);
            } catch (ClassNotFoundException ce) {
                System.err.println("unable to parse message: " + ce.getMessage());
            } catch (SocketException e) {
                System.err.println("connection error: " + e.getMessage());
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("unable to read message :" + e.getMessage());
            }
            return str;
        }

        void close() throws Exception {
            in.close();
            out.close();
            socket.close();
        }
    }
}
