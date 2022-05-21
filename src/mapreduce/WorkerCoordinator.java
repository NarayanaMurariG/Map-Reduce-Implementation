package mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.*;


/*
    These are threads to coordinate with worker processes independently and parallelly
*/
public class WorkerCoordinator implements Callable<String> {

    private int workerPort;
    private long startLine;
    private long endLine;
    private String fileName;
    private String intermediateFile;
    private ProcessType processType;
    private String finalOutputFile;
    private UseCase useCase;
    private String inputParameter = null;

    public void setInputParameter(String inputParameter) {
        this.inputParameter = inputParameter;
    }



    public WorkerCoordinator(int workerPort, long startLine, long endLine, String fileName,ProcessType processType,UseCase useCase) {
        this.workerPort = workerPort;
        this.startLine = startLine;
        this.endLine = endLine;
        this.fileName = fileName;
        this.processType = processType;
        this.useCase = useCase;
    }

    public WorkerCoordinator(int workerPort, String intermediateFile, ProcessType processType, UseCase useCase) {
        this.workerPort = workerPort;
        this.intermediateFile = intermediateFile;
        this.processType = processType;
        this.useCase = useCase;
    }

    public void setIntermediateFile(String intermediateFile) {
        this.intermediateFile = intermediateFile;
    }

    @Override
    public String call() throws Exception {
        InetAddress ip = InetAddress.getByName("localhost");
        Socket socket = new Socket(ip,workerPort);

        // Based on the either map or reduce phase, these worker coordinators act accordingly and communicate with
        // worker nodes

        // obtaining input and out streams
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        String incomingMessage;
        if(processType.equals(ProcessType.MAP)){
            dos.writeUTF(Constants.MAP_PHASE);
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF(fileName);
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF(Long.toString(startLine));
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF(Long.toString(endLine));
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            //Sending Use case
            dos.writeUTF(useCase.toString());
            incomingMessage = dis.readUTF();

            if(useCase.equals(UseCase.DISTRIBUTED_GREP)){
                dos.writeUTF(inputParameter);
                incomingMessage = dis.readUTF();
            }


            dos.writeUTF("Send me intermediateFile Name");
            intermediateFile = dis.readUTF();
//            System.out.println(intermediateFile);

            dos.writeUTF("Close Connection");
            return intermediateFile;
        }else {
            dos.writeUTF(Constants.REDUCE_PHASE);
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF(intermediateFile);
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF(useCase.toString());
            incomingMessage = dis.readUTF();
//            System.out.println(incomingMessage);

            dos.writeUTF("Send me Final Output File Name");
            finalOutputFile = dis.readUTF();
//            System.out.println(finalOutputFile);

            dos.writeUTF("Close Connection");
            return finalOutputFile;
        }

    }

}
