package mapreduce;

import com.sun.tools.internal.jxc.ap.Const;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {
    public static void main(String[] args){
        if(args.length < 2){
            System.out.println("Please enter the input filename and use case");
            System.exit(0);
        }else{
            try{
                String inputFile = args[0];
                String useCase = args[1];

                String outputFile = sendRequest(inputFile,useCase);

                System.out.println("Final Output Files : "+outputFile);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private static String sendRequest(String inputFile, String useCase) throws IOException {
        InetAddress ip = InetAddress.getByName("localhost");
        Socket socket = new Socket(ip, Constants.MASTER_PORT);

        // obtaining input and out streams
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        System.out.println("Input File : "+inputFile);
        System.out.println("Use Case : "+useCase);
        System.out.println("Sending request to master server....");
        dos.writeUTF(inputFile);
        dis.readUTF();

        dos.writeUTF(useCase);
        dis.readUTF();

        dos.writeUTF("Send output file");
        String outputFile = dis.readUTF();

        dos.writeUTF("Close");

        return outputFile;
    }
}
