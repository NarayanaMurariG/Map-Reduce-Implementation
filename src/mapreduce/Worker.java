package mapreduce;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

public class Worker {

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

    /*
     * public ProcessType getProcessType() {
     * return processType;
     * }
     * 
     * public void setProcessType(ProcessType processType) {
     * this.processType = processType;
     * }
     * 
     * public String getFilePath() {
     * return filePath;
     * }
     * 
     * public void setFilePath(String filePath) {
     * this.filePath = filePath;
     * }
     * 
     * public int getStartByte() {
     * return startLineNo;
     * }
     * 
     * public void setStartByte(int startByte) {
     * this.startLineNo = startByte;
     * }
     * 
     * public int getEndByte() {
     * return endLineNo;
     * }
     * 
     * public void setEndByte(int endByte) {
     * this.endLineNo = endByte;
     * }
     */

    public static void main(String[] args) {
        workerNo = Integer.parseInt(args[0]);
        try {
            startServer(args);
            fetchInstructionsFromMaster();
        } catch (Exception e) {
            System.out.println("Exception Occured In Worker Thread");
            e.printStackTrace();
        }

    }

    private static void startServer(String[] args) throws Exception {
        serverPort = Constants.BASE_WORKER_PORT + (workerNo - 1) * 20;
        System.out.println("Starting worker port on : " + serverPort);
        server = new ServerSocket(serverPort);
    }

    private static void startReducePhase(DataInputStream inputStream, DataOutputStream outputStream) throws Exception {

        filePath = inputStream.readUTF(); // List of intermediate files converted to string
        System.out.println("Working with intermediate files : " + filePath);
        outputStream.writeUTF(Constants.OK);

        filePath = filePath.substring(1);
        int length = filePath.length();
        filePath = filePath.substring(0, length - 1);
        System.out.println("Trimmed File Path :" + filePath);
        String[] intermediateFiles = filePath.split(",");

        String useCase = inputStream.readUTF();
        System.out.println("Usecase : " + useCase);
        outputStream.writeUTF(Constants.OK);

        // Send response to master
        String incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : " + incomingMessage);
        String outgoingMessage = generateWordCountFinalFile(intermediateFiles, UseCase.valueOf(useCase));
        outputStream.writeUTF(outgoingMessage);

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : " + incomingMessage);

        // close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();

    }

    private static String generateWordCountFinalFile(String[] intermediateFiles, UseCase useCase)
            throws ExecutionException, InterruptedException, IOException {
        // Code for word count
        Map keyMapping = new ConcurrentHashMap();
        if (useCase.equals(UseCase.WORD_COUNT)) {
            ExecutorService executors = Executors.newFixedThreadPool(intermediateFiles.length);
            FutureTask[] futures = new FutureTask[intermediateFiles.length];

            for (int i = 0; i < intermediateFiles.length; i++) {
                futures[i] = new FutureTask(
                        new ReduceCoordinator(intermediateFiles[i], UseCase.WORD_COUNT, keyMapping, workerNo));
                executors.submit(futures[i]);
            }

            for (int i = 0; i < intermediateFiles.length; i++) {
                futures[i].get();
            }

            return writeHashMapToFile(keyMapping);

        } else {
            return null;
        }
    }

    private static String writeHashMapToFile(Map keyMapping) throws IOException {

        String filePath = "output-" + workerNo + ".txt";
        Path outputFilePath = Paths.get(filePath);
        BufferedWriter bw = Files.newBufferedWriter(outputFilePath);

        for (Object key : keyMapping.keySet()) {
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

    private static void fetchInstructionsFromMaster() throws Exception {

        String incomingMessage;
        while (true) {
            socket = server.accept();

            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());

            incomingMessage = inputStream.readUTF();

            if (incomingMessage.equals(Constants.MAP_PHASE)) {
                outputStream.writeUTF(Constants.OK);
                startMapPhase(inputStream, outputStream);
            } else {
                outputStream.writeUTF(Constants.OK);
                startReducePhase(inputStream, outputStream);
            }

        }
    }

    private static void startMapPhase(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException, Exception {

        String incomingMessage, outgoingMessage;
        filePath = inputStream.readUTF();
        System.out.println("Working with file : " + filePath);
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        startLineNo = Integer.parseInt(incomingMessage);
        System.out.println("Start Line (Exclusive) : " + startLineNo);
        outputStream.writeUTF(Constants.OK);

        incomingMessage = inputStream.readUTF();
        endLineNo = Integer.parseInt(incomingMessage);
        System.out.println("End Line (Inclusive) : " + endLineNo);
        outputStream.writeUTF(Constants.OK);

        // TODO Now perform map phase and generate Intermediate File

        // Send response to master
        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : " + incomingMessage);
        // outgoingMessage = "IntermediateFile.txt";
        outgoingMessage = generateWordCountIntermediateFile(filePath, startLineNo, endLineNo);
        outputStream.writeUTF(outgoingMessage);

        incomingMessage = inputStream.readUTF();
        System.out.println("Message from Client : " + incomingMessage);

        // close streams and socket
        inputStream.close();
        outputStream.close();
        socket.close();
    }

    private static String generateDistributedGrepIntermediateFile(String inputfilePath, int startLineNo, int endLineNo,
            String action, String pattern) throws Exception {
        String filePathIntermediate = "intermediateFile-" + action + "-" + workerNo + ".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : " + startLineNo + " End Line : " + endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath, "r");
        String line;
        skipLines(file, startLineNo);
        int count = startLineNo;
        while ((line = file.readLine()) != null && count < endLineNo) {
            if (line.contains(pattern)) {
                intermediateFileWriter.write(line + "," + "1");
                intermediateFileWriter.newLine();
            }
            count++;
        }
        intermediateFileWriter.close();
        return filePathIntermediate;
    }

    private static String generateReverseWebLinkIntermediateFile(String inputfilePath, int startLineNo, int endLineNo,
            String action) throws Exception {
        String filePathIntermediate = "intermediateFile-" + action + "-" + workerNo + ".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : " + startLineNo + " End Line : " + endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath, "r");
        String line;
        skipLines(file, startLineNo);
        int count = startLineNo;
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

    private static String generateWordCountIntermediateFile(String inputfilePath, int startLineNo, int endLineNo)
            throws Exception {
        String filePathIntermediate = "intermediateFile-" + workerNo + ".txt";
        Path intermediateFilePath = Paths.get(filePathIntermediate);
        BufferedWriter intermediateFileWriter = Files.newBufferedWriter(intermediateFilePath);
        System.out.println("Start Line : " + startLineNo + " End Line : " + endLineNo);

        RandomAccessFile file = new RandomAccessFile(inputfilePath, "r");
        String line;
        skipLines(file, startLineNo);
        int count = startLineNo;
        while ((line = file.readLine()) != null && count < endLineNo) {
            line = line.replaceAll("\\p{Punct}", "");
            line = line.replaceAll("\\s+", " ");
            // System.out.println(line);

            String[] words = line.split(" ");

            for (String word : words) {
                intermediateFileWriter.write(word.toLowerCase() + "," + "1");
                intermediateFileWriter.newLine();
            }
            count++;
        }

        intermediateFileWriter.close();
        return filePathIntermediate;
    }

    /*
     * private static void reducephaseone(String inputfilePath) throws Exception {
     * new Thread(new Runnable() {
     * 
     * @Override
     * public void run() {
     * try {
     * BufferedReader br = new BufferedReader(new FileReader(inputfilePath));
     * String line = br.readLine();
     * while (line != null) {
     * 
     * String[] words = line.split(",");
     * String key = words[0];
     * int value = Integer.parseInt(words[0]);
     * 
     * int worker = (key.hashCode()) % 3 + 1;
     * if (worker == workerNo) {
     * // KeyMapping.computeIfAbsent(key, 1);
     * keyMapping.putIfAbsent(key, value);
     * keyMapping.computeIfPresent(key, (k, v) -> (int) v + 1);
     * }
     * 
     * // read next line
     * line = br.readLine();
     * }
     * br.close();
     * } catch (Exception e) {
     * e.printStackTrace();
     * }
     * }
     * }).start();
     * }
     * 
     * private static void reducephasetwo() throws Exception, IOException {
     * 
     * String filePath = "output-" + workerNo + ".txt";
     * Path outputFilePath = Paths.get(filePath);
     * BufferedWriter bw = Files.newBufferedWriter(outputFilePath);
     * 
     * keyMapping.forEach((k, v) -> {
     * System.out.printf("    k: %s, v: %s%n", k, v);
     * try {
     * bw.write(k + "," + v);
     * } catch (IOException e) {
     * 
     * e.printStackTrace();
     * }
     * });
     * }
     */

    private static void skipLines(RandomAccessFile file, int startLineNo) throws Exception {

        for (int i = 0; i < startLineNo; i++) {
            file.readLine();
        }

    }
}
