package mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.*;

public class WorkerCoordinator implements Callable<String> {

    private int workerPort;
    private long startLine;
    private long endLine;
    private String fileName;
    private String intermediateFile;

    public WorkerCoordinator(int workerPort, long startLine, long endLine, String fileName) {
        this.workerPort = workerPort;
        this.startLine = startLine;
        this.endLine = endLine;
        this.fileName = fileName;
    }

    @Override
    public String call() throws Exception {
        InetAddress ip = InetAddress.getByName("localhost");
        Socket socket = new Socket(ip,workerPort);

        // obtaining input and out streams
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        dos.writeUTF(Constants.MAP_PHASE);
        System.out.println(dis.readUTF());

        dos.writeUTF(fileName);
        System.out.println(dis.readUTF());

        dos.writeUTF(Long.toString(startLine));
        System.out.println(dis.readUTF());

        dos.writeUTF(Long.toString(endLine));
        System.out.println(dis.readUTF());

        dos.writeUTF("Send me intermediateFile Name");
        intermediateFile = dis.readUTF();
        System.out.println(intermediateFile);

        dos.writeUTF("Close Connection");
        return intermediateFile;
    }
}
