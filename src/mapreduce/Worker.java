package mapreduce;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

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
    private static Map keyMapping = new ConcurrentHashMap();


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


    //Starting the server
    private static void startServer(String[] args) throws Exception{
            serverPort = Constants.BASE_WORKER_PORT + (workerNo - 1) * 20;
            System.out.println("Starting worker port on : "+serverPort);
            server = new ServerSocket(serverPort);
    }

    private static void startReducePhase(DataInputStream inputStream, DataOutputStream outputStream) throws Exception{


        filePath = inputStream.readUTF(); //List of intermediate files converted to string
        System.out.println("Working with intermediate files : "+filePath);
        outputStream.writeUTF(Constants.OK);

        filePath = filePath.substring(1);
        int length = filePath.length();
        filePath = filePath.substring(0,length-1);
        System.out.println("Trimmed File Path :"+ filePath);
        String[] intermediateFiles = filePath.split(",");

        String useCase = inputStream.readUTF();
        System.out.println("Usecase : "+ useCase);
        outputStream.writeUTF(Constants.OK);

        //Send response to master
        String incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);
        String outgoingMessage = generateFinalOutputFile(intermediateFiles,UseCase.valueOf(useCase));
        outputStream.writeUTF(outgoingMessage);

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);

        //close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();

    }

    private static String generateFinalOutputFile(String[] intermediateFiles, UseCase useCase) throws ExecutionException, InterruptedException, IOException {
        /*
            Generates the final output file using Reduce workers which independently run through
            every intermediate file and check the keys based on hashcode and then update the final map
            containing the final key value pairs of final output
        */
        Map keyMapping = new ConcurrentHashMap();

        ExecutorService executors = Executors.newFixedThreadPool(intermediateFiles.length);
        FutureTask[] futures = new FutureTask[intermediateFiles.length];

        for(int i=0;i<intermediateFiles.length;i++){
            futures[i] = new FutureTask(new ReduceCoordinator(intermediateFiles[i],useCase,keyMapping,workerNo));
            executors.submit(futures[i]);
        }

        for(int i=0;i<intermediateFiles.length;i++){
            futures[i].get();
        }

        return writeHashMapToFile(keyMapping);
    }


    private static String generateDistributedGrepIntermediateFile(String inputfilePath, int startLineNo, int endLineNo,String pattern) throws Exception {
        String filePathIntermediate = "intermediateFile-" + workerNo + ".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : " + startLineNo + " End Line : " + endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath, "r");
        String line;
        skipLines(file, startLineNo);
        int count = startLineNo;
        // Checks if the line contains the pattern and emits that line number and line into intermediate file
        while ((line = file.readLine()) != null && count < endLineNo) {
            if (line.contains(pattern)) {
                intermediateFileWriter.write(count+1 + "," + line);
                intermediateFileWriter.newLine();
            }
            count++;
        }
        intermediateFileWriter.close();
        return filePathIntermediate;
    }

    private static String generateReverseWebLinkIntermediateFile(String inputfilePath, int startLineNo, int endLineNo) throws Exception {
        String filePathIntermediate = "intermediateFile-" + workerNo + ".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : " + startLineNo + " End Line : " + endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath, "r");
        String line;
        skipLines(file, startLineNo);
        int count = startLineNo;
        //Emits the target, source pairs from source, target pair
        while ((line = file.readLine()) != null && count < endLineNo) {
            // input line = source -> target
            String[] words = line.split("->");
            // output line = target,source
            intermediateFileWriter.write(words[1] + "," + words[0]);
            intermediateFileWriter.newLine();
            count++;
        }
        intermediateFileWriter.close();
        return filePathIntermediate;
    }

    //Writes the final output into a output file for each worker node
    private static String writeHashMapToFile(Map keyMapping) throws IOException {

        String filePath = "output-" + workerNo + ".txt";
        Path outputFilePath = Paths.get(filePath);
        BufferedWriter bw = Files.newBufferedWriter(outputFilePath);


        for(Object key : keyMapping.keySet()){
            try {
                bw.write(key.toString() + "," + keyMapping.get(key));
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        bw.close();
        return filePath;
    }

    private static void fetchInstructionsFromMaster() throws Exception{

        String incomingMessage;

        // Waiting on incoming requests from Masters and will act accordingly based on either Map phase or Reduce Phase
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

        String incomingMessage,outgoingMessage = null;
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

        //Getting usecase
        String useCase = inputStream.readUTF();
        outputStream.writeUTF(Constants.OK);

        //Send response to master
        String pattern = null;
        if(useCase.equals(UseCase.DISTRIBUTED_GREP.toString())){
            pattern = inputStream.readUTF();
            System.out.println("Pattern for GREP : " + pattern);
            outputStream.writeUTF(Constants.OK);
        }

        /*
            Based on usecase appropriate function will be triggered which will generate the intermediate files
        */
        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);
        if(useCase.equals(UseCase.WORD_COUNT.toString())){
            outgoingMessage = generateWordCountIntermediateFile(filePath,startLineNo,endLineNo);
            outputStream.writeUTF(outgoingMessage);
        }else if (useCase.equals(UseCase.REVERSE_WEB_LINK.toString())){
            outgoingMessage = generateReverseWebLinkIntermediateFile(filePath,startLineNo,endLineNo);
            outputStream.writeUTF(outgoingMessage);
        }else if (useCase.equals(UseCase.DISTRIBUTED_GREP.toString())){
            outgoingMessage = generateDistributedGrepIntermediateFile(filePath,startLineNo,endLineNo,pattern);
            outputStream.writeUTF(outgoingMessage);
        }

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : "+incomingMessage);

        //close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();
    }

    private static String generateWordCountIntermediateFile(String inputfilePath,int startLineNo, int endLineNo) throws Exception{
        String filePathIntermediate = "intermediateFile-"+workerNo+".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : "+startLineNo+" End Line : "+endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath,"r");
        String line;
        skipLines(file,startLineNo);
        int count = startLineNo;

        //Removing all puntuations and extra empty spaces and then tokenize the words and write to intermediate files
        while((line = file.readLine()) != null && count < endLineNo){
            line = line.replaceAll("\\p{Punct}", "");
            line = line.replaceAll("\\s+", " ");
//            System.out.println(line);

            String[] words = line.split(" ");

            for(String word : words){
                intermediateFileWriter.write(word.toLowerCase() + "," + "1");
                intermediateFileWriter.newLine();
            }
            count++;
        }

        intermediateFileWriter.close();
        return filePathIntermediate;
    }

    private static void skipLines(RandomAccessFile file, int startLineNo) throws Exception{

        for(int i=0;i<startLineNo;i++){
            file.readLine();
        }

    }
}
