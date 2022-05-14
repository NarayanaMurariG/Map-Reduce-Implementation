package mapReduce;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class Worker{

    private static ProcessType processType;
    private static int workerNo;
    private static String filePath;
    private static int startLineNo;
    private static int endLineNo;
    private static ServerSocket server;
    private static Socket socket;
    private static int serverPort;
    private static DataInputStream inputStream;
    private static DataOutputStream outputStream;

    public ProcessType getProcessType() {
        return processType;
    }

    public void setProcessType(ProcessType processType) {
        this.processType = processType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getStartByte() {
        return startLineNo;
    }

    public void setStartByte(int startByte) {
        this.startLineNo = startByte;
    }

    public int getEndByte() {
        return endLineNo;
    }

    public void setEndByte(int endByte) {
        this.endLineNo = endByte;
    }

    public static void main(String[] args) {
        workerNo = Integer.parseInt(args[0]);
        try {
            startServer(args);
            fetchInstructionsFromMaster();
        }catch (Exception e){
            System.out.println("Exception Occured In Worker Thread");
            e.printStackTrace();
        }

    }

    private static void startServer(String[] args) throws Exception{

            if(args.length < 2){
                serverPort = Constants.DEFAULT_SERVER_PORT;
            }else{
                serverPort = Integer.parseInt(args[2]);
            }
            System.out.println("Starting worker port on : "+serverPort);
            server = new ServerSocket(serverPort);
    }

    private static void startReducePhase(DataInputStream inputStream, DataOutputStream outputStream) throws Exception{

        String incomingMessage,outgoingMessage;
        incomingMessage = inputStream.readUTF();
        List<String> intermediateFilePaths = Arrays.asList(incomingMessage.split(","));
        System.out.println("Working with intermediate files : "+ intermediateFilePaths.toString());
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        startLineNo = Integer.parseInt(incomingMessage);
        System.out.println("Start Line (Exclusive) : "+ startLineNo);
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        endLineNo = Integer.parseInt(incomingMessage);
        System.out.println("End Line (Inclusive) : "+ endLineNo);
        outputStream.writeUTF(Constants.OK);

        //TODO Now perform map phase and generate Intermediate File

        //Send response to master
        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);
//        outgoingMessage = "IntermediateFile.txt";
        outgoingMessage = generateWordCountIntermediateFile(filePath,startLineNo,endLineNo);
        outputStream.writeUTF(outgoingMessage);

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);

        //close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();

    }

    private static void startMapPhase(UseCase useCase) throws Exception{

//        TODO Read File Line By Line and split by space, write key,value pair to intermediate file


    }

    private static void hostServer() {
//        TODO Start Server
    }

    private static void fetchInstructionsFromMaster() throws Exception{

//        TODO Wait for Master To Send Instructions
        String incomingMessage,outgoingMessage;
        while(true){
            socket = server.accept();

            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());

            incomingMessage = inputStream.readUTF();

            if(incomingMessage.equals(Constants.MAP_PHASE)){
                outputStream.writeUTF(Constants.OK);
                startMapPhase(inputStream,outputStream);
            }else{
                outputStream.writeUTF(Constants.OK);
                startReducePhase(inputStream,outputStream);
            }

        }
    }

    private static void startMapPhase(DataInputStream inputStream, DataOutputStream outputStream) throws IOException ,Exception{

        String incomingMessage,outgoingMessage;
        filePath = inputStream.readUTF();
        System.out.println("Working with file : "+filePath);
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        startLineNo = Integer.parseInt(incomingMessage);
        System.out.println("Start Line (Exclusive) : "+ startLineNo);
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        endLineNo = Integer.parseInt(incomingMessage);
        System.out.println("End Line (Inclusive) : "+ endLineNo);
        outputStream.writeUTF(Constants.OK);

        //TODO Now perform map phase and generate Intermediate File

        //Send response to master
        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);
//        outgoingMessage = "IntermediateFile.txt";
        outgoingMessage = generateWordCountIntermediateFile(filePath,startLineNo,endLineNo);
        outputStream.writeUTF(outgoingMessage);

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);

        //close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();
    }

    private static String generateWordCountIntermediateFile(String filePath, int startLineNo, int endLineNo) throws Exception{

        Path intermediateFilePath = Paths.get("intermediateFile.txt");
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);

        Path path = Paths.get(filePath);
        Stream lines = Files.lines(path);
        lines.skip(startLineNo);
        String line;
        int count = 0;
        for (Iterator it = lines.iterator(); it.hasNext(); ) {
            line = (String) it.next();
            line = line.replaceAll("\\p{Punct}", "");
            line = line.replaceAll("\\s+", " ");
            System.out.println(line);

            String[] words = line.split(" ");

            for(String word : words){
                intermediateFileWriter.write(word.toLowerCase() + "," + "1");
                intermediateFileWriter.newLine();
            }

            if(++count == endLineNo){
                break;
            }
        }

        intermediateFileWriter.close();
        return "intermediateFile";
    }
}
