package mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class MasterProcess {

    private static String fileName;
    private static UseCase useCase;
    private static Setup setup;
    private static int port;
    private static Socket socket;
    private static ServerSocket ssock;

    public static void main(String[] args) throws Exception {
        fileName = args[0];

        if (args[1].equalsIgnoreCase(UseCase.WORD_COUNT.toString())) {
            useCase = UseCase.WORD_COUNT;
        } else {
            useCase = UseCase.REVERSE_WEB_LINK;
        }

        demo(args[2], useCase);
        // startMapPhase(useCase);
    }

    /**
     * @author Haravind Rajula
     * @param tcp
     * @param useCase
     * @throws Exception
     */
    private static void demo(String tcp, UseCase useCase) throws Exception {
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ssock = new ServerSocket(Integer.parseInt(tcp));
                    socket = ssock.accept();
                    startMapPhase(useCase);
                } catch (Exception e) {

                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void startMapPhase(UseCase useCase) throws Exception {

        // long fileSize = Utils.getFileSize(fileName);
        // System.out.println("File Size is : "+fileSize);
        // long chunk_size = fileSize / 3;

        long totalNoOfLines = Utils.getTotalNoOfLines(fileName);
        System.out.println("Total No Of Lines : " + totalNoOfLines);

        InetAddress ip = InetAddress.getByName("localhost");
        socket = new Socket(ip, 8080);

        // obtaining input and out streams
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        dos.writeUTF(Constants.MAP_PHASE);
        System.out.println(dis.readUTF());

        dos.writeUTF(fileName);
        System.out.println(dis.readUTF());

        dos.writeUTF(Long.toString(0));
        System.out.println(dis.readUTF());

        dos.writeUTF(Long.toString(totalNoOfLines / 2));
        System.out.println(dis.readUTF());

        dos.writeUTF("Send me intermediateFile Name");
        System.out.println(dis.readUTF());

        dos.writeUTF("Close Connection");
    }

}
