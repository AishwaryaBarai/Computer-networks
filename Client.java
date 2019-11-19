import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {
    Map<Integer, Integer> chuckInfo = new HashMap<>();
    private Socket requestSocket;           //socket connect to the server
    private Path currentRelativePath = Paths.get("");
    private String currentDir = currentRelativePath.toAbsolutePath().toString() + "/";
    private int pubPort = 2222;
    private int subPort = 2220;
    private int serverPort = 8000;
    private String host = "localhost";
    private String peerId;
    private List<String> needChunks = new ArrayList<>();
    private Map<String,Boolean> haveChunks = new HashMap<>();
    private List<String> knownPeers = new ArrayList<>();
    private String fileName="test.pdf";

    public static void main(String[] args) {
        String host = "localhost";
        int sPort = 8000;
        if (args.length < 2) {
            System.out.println("Command line arguments were not provided. Setting to default values: localhost,8000");
        }
        Client client = new Client();
        client.run(args[0], args[1]);
    }

    private void run(String publishPort, String knownPeer) {
        String input;
        try {
            pubPort=Integer.parseInt(publishPort);
            knownPeers.add(knownPeer);
            requestSocket = new Socket("localhost", 8000);
            SocketUtil socketUtil = new SocketUtil(requestSocket);
//            System.out.println("Connected to " + host + " in port " + port);
            socketUtil.sendMessage("hello");
            if (socketUtil.readMessage().equals("hello")) {
                System.out.println("server: hello");
            } else {
                System.err.println("connection error with server; please restart the client and try again");
            }
            socketUtil.sendMessage("get peerId");
            peerId = socketUtil.readMessage();
            System.out.println("requesting chunk info..");
            try {
                socketUtil.sendMessage("get chunkInfo");
                chuckInfo = (Map) socketUtil.getIn().readObject();
                socketUtil.sendMessage("success");
            } catch (Exception e) {
                System.out.println("unable to parse chuck info" + e.getMessage());
                System.exit(0);
            }
            for (int chunkId : chuckInfo.keySet()) {
                needChunks.add(String.valueOf(chunkId));
            }
            System.out.println("chunk info fetched successfully! " + chuckInfo.get(1));
            System.out.println("requesting chunks from the server..");
            try {
                socketUtil.sendMessage("get chunks");
                if (!socketUtil.readMessage().equals("sending chunks")) {
                    System.out.println("error while receiving chunks from server");
                    return;
                }
                socketUtil.sendMessage("ok");
                String str = socketUtil.readMessage();
                System.out.println("startin to receive chunk" + str);
                while (str.equals("") || !str.equals("complete")) {
                    Path path = Paths.get(currentDir+"_tmp" + str);
                    Files.copy(socketUtil.getIn(), path, StandardCopyOption.REPLACE_EXISTING);
                    //todo checksum validation
                    needChunks.remove(str);
                    haveChunks.put(str,false);
                    str = socketUtil.readMessage();
                }
                socketUtil.sendMessage("success");
                System.out.println("disconnecting from server..");
                socketUtil.close();
                Thread t1 = new Thread(new Runnable() {
                    public void run()
                    {
                        receiveChunks();
                    }});
                t1.start();
                publishChunks();
                FileOutputStream fos=new FileOutputStream(currentDir+fileName);
                for(Integer chunkId:chuckInfo.keySet()){
                    System.out.println(chunkId);
                    fos.write(Files.readAllBytes(Paths.get(currentDir+"_tmp"+chunkId)));
                    fos.flush();
                }
                fos.close();
            } catch (Exception e) {
                System.out.println("error while receiving file from the server: " + e.getMessage());
                e.printStackTrace();
            }
            System.out.println("chunks retrieved from the server successfully");
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
        try {
            Socket socket = null;
            SocketUtil socketUtil;
            boolean flag = true;
            while (flag) {
                for (String portStr : knownPeers) {
                    flag = false;
                    try {
                        socket = new Socket(host, Integer.parseInt(portStr));
                        System.out.println("connected to peer successfully");
                        socketUtil = new SocketUtil(socket);
                        socketUtil.sendMessage("hello");
                        if (!socketUtil.readMessage().equals("hello")) {
                            System.out.println("something wrong; disconnecting peer..");
                            return;
                        }
                        socketUtil.sendMessage("i'm: " + peerId + " host " + host + " port " + pubPort);
                        if (!socketUtil.readMessage().equals("ok")) {
                            System.out.println("something wrong; disconnecting peer..");
                            return;
                        }
                        List<String> chunks = new ArrayList<>();
                        for (String chunkId : needChunks) {
                            socketUtil.sendMessage("need " + chunkId);
                            String str=socketUtil.readMessage();
                            if (str.equals("i have")) {
                                System.out.println("fetching chunk-"+chunkId+"..");
                                Path path = Paths.get(currentDir+"_tmp" + chunkId);
                                Files.copy(socketUtil.getIn(), path, StandardCopyOption.REPLACE_EXISTING);
                                System.out.println("received chunk:"+chunkId+" successfully");
                                System.out.println(socketUtil.readMessage());
                                socketUtil.sendMessage("success");
                                chunks.add(chunkId);
                                Thread.sleep(1000);
                            }
                        }
                        for (String chunkId : chunks) {
                            needChunks.remove(chunkId);
                            haveChunks.put(chunkId,false);
                        }
                        socketUtil.sendMessage("bye");
                        if (!needChunks.isEmpty())
                            flag = true;
                    } catch (Exception ioException) {
                        flag = true;
                        System.out.println("failed connect to peer in port:" + portStr);
                        Thread.sleep(5000);
                    }
                }
            }
            System.out.println("i have all chunk with me; combining chunks..");
            FileOutputStream fos=new FileOutputStream(currentDir+fileName);
            for(Integer chunkId:chuckInfo.keySet()){
                System.out.println(chunkId);
                fos.write(Files.readAllBytes(Paths.get(currentDir+"_tmp"+chunkId)));
                fos.flush();
            }
            fos.close();
            System.out.println("FINAL FILE IS READY !!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void publishChunks() {
        ServerSocket pubSocket;
        SocketUtil socketUtil;
        try {
            InetAddress addr = InetAddress.getByName(host);
            pubSocket = new ServerSocket(pubPort, 10, addr);
            while (true) {
                System.out.println("waiting for new peer requests..");
                socketUtil = new SocketUtil(pubSocket.accept());
                System.out.println("connection accepted!");
                if (!socketUtil.readMessage().equals("hello")) {
                    System.out.println("something wrong; disconnecting peer..");
                    return;
                }
                socketUtil.sendMessage("hello");
                String[] strs = socketUtil.readMessage().split(" ");
                if (strs.length < 6) {
                    System.out.println("something wrong; disconnecting peer..");
                    return;
                }
                if (!knownPeers.contains(strs[5])) {
                    System.out.println("new peer identified: host:" + strs[3] + " port:" + strs[5]);
                    knownPeers.add(strs[5]);
                }
                socketUtil.sendMessage("ok");
                String req = socketUtil.readMessage();
                while (req.startsWith("need")) {
                    if (req.split(" ").length < 2)
                        continue;
                    else if (haveChunks.containsKey(req.split(" ")[1])) {
                        socketUtil.sendMessage("i have");
                        Path path = Paths.get(currentDir+"_tmp" + req.split(" ")[1]);
                        Files.copy(path, socketUtil.getOut());
                        socketUtil.sendMessage("sent chunk_"+req.split(" ")[1]);
                        if (!socketUtil.readMessage().equals("success")) {
                            System.out.println("something wrong; disconnecting peer..");
                            return;
                        }else {
                            haveChunks.put(req.split(" ")[1],true);
                        }
                        Thread.sleep(1000);
                    }
                    req = socketUtil.readMessage();
                }
                if(!haveChunks.containsValue(false)){
                    System.out.println("I have sent all chunks to atleast one other peer. So bye bye..");
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to start the server:" + e.getMessage());
        }
    }




    public class SocketUtil {
        private Socket socket;           //socket connect to the server
        private ObjectOutputStream out;         //stream write to the socket
        private ObjectInputStream in;          //stream read from the socket

        public ObjectOutputStream getOut() {
            return out;
        }

        public ObjectInputStream getIn() {
            return in;
        }

        public SocketUtil(Socket socket) {
            this.socket = socket;
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        void sendMessage(String msg) {
            try {
                System.out.println("Me: " + msg);
                out.writeObject(msg);
                out.flush();
            } catch (Exception e) {
                System.out.println("Exception occurred while sending message:"+e.getMessage());
            }
        }

        void sendObject(Object o) {
            try {
                out.writeObject(o);
                out.flush();
            } catch (Exception e) {
                System.out.println("exception occurred while sending object"+e.getMessage());
            }
        }

        String readMessage() throws Exception{
            String str = "";
            try {
                str = (String) in.readObject();
                System.out.println("remote: "+str);
            } catch (ClassNotFoundException ce) {
                System.err.println("unable to parse message: "+ce.getMessage());
            } catch (SocketException e) {
                System.err.println("connection error: "+e.getMessage());
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("unable to read message :"+e.getMessage() );
            }
            return str;
        }

        void close() throws Exception{
            in.close();
            out.close();
            socket.close();
        }
    }
}
