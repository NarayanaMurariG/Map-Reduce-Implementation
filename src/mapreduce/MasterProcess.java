package mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class MasterProcess {

    private static String fileName;
    private static UseCase useCase;
    private static Setup setup;
    private static int port;
    private static ServerSocket server;

    public static void main(String[] args) throws Exception{

//        startServerAndListen();


        fileName = args[0];

        if(args[1].equalsIgnoreCase(UseCase.WORD_COUNT.toString())){
            useCase = UseCase.WORD_COUNT;
        }else{
            useCase = UseCase.REVERSE_WEB_LINK;
        }

        List<String> intermediateFiles = startMapPhase(useCase);
        System.out.println(intermediateFiles.toString());
        List<String> outputFiles = startReducePhase(useCase,intermediateFiles);
        System.out.println("The final generated output files are : "+outputFiles.toString());
//        generateFileOutputFile(outputFiles);
    }

    private static void generateFileOutputFile(List<String> outputFiles) {
    }

    private static List<String> startReducePhase(UseCase useCase,List<String> intermediateFiles) throws ExecutionException, InterruptedException {

        ExecutorService executors = Executors.newFixedThreadPool(Constants.MASTER_THREAD_POOL_SIZE);

        FutureTask<String>[] futureTasks = new FutureTask[Constants.SLAVE_COUNT];
        int workerPort;
        for(int i=1;i<=Constants.SLAVE_COUNT;i++){
            workerPort = getWorkerPortNumber(i);
            futureTasks[i-1] = new FutureTask(new WorkerCoordinator(workerPort,intermediateFiles.toString(),ProcessType.REDUCE,UseCase.WORD_COUNT));
            executors.submit(futureTasks[i-1]);
        }

        List<String> outputFiles = new ArrayList<>();
        for(int i=0;i<Constants.SLAVE_COUNT;i++) {
            outputFiles.add(futureTasks[i].get());
        }
        return outputFiles;

    }

    private static void startServerAndListen(){
        try {
            server = new ServerSocket(Constants.MASTER_PORT);
            while(true){
                Socket socket = server.accept();

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                String inputFile = dis.readUTF();
                dos.writeUTF(Constants.OK);

                String useCase = dis.readUTF();
                dos.writeUTF(Constants.OK);

                System.out.println(dis.readUTF());
                //TODO Send to workers and get final file
                String finalFile = "finalFile";
                dos.writeUTF(finalFile);

                System.out.println(dis.readUTF());

                dis.close();
                dos.close();
                socket.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private static List<String> startMapPhase(UseCase useCase) throws Exception{

        long totalNoOfLines = Utils.getTotalNoOfLines(fileName);
        System.out.println("Total No Of Lines : "+totalNoOfLines);

        ExecutorService executors = Executors.newFixedThreadPool(Constants.MASTER_THREAD_POOL_SIZE);

        FutureTask<String>[] futureTasks = new FutureTask[Constants.SLAVE_COUNT];
        int workerPort;
        long startLine, endLine;
        for(int i=1;i<=Constants.SLAVE_COUNT;i++){
            workerPort = getWorkerPortNumber(i);
            startLine = getStartLineNo(i,totalNoOfLines);
            endLine = getEndLineNo(i,totalNoOfLines);
            futureTasks[i-1] = new FutureTask(new WorkerCoordinator(workerPort,startLine,endLine,fileName,ProcessType.MAP,UseCase.WORD_COUNT));
            executors.submit(futureTasks[i-1]);
        }

        List<String> intermediateFiles = new ArrayList<>();
        for(int i=0;i<Constants.SLAVE_COUNT;i++) {
            intermediateFiles.add(futureTasks[i].get());
        }
        return intermediateFiles;
    }

    private static long getEndLineNo(int workerNo, long totalNoOfLines) {
        if(totalNoOfLines < 3 && workerNo > 1){
            return 0;
        }else if(workerNo == Constants.SLAVE_COUNT){
            return totalNoOfLines;
        } else{
            return (totalNoOfLines / 3) * (workerNo);
        }
    }

    private static long getStartLineNo(int workerNo, long totalNoOfLines) {
//        System.out.println("Worker No : "+workerNo+" , startLine : "+(totalNoOfLines / 3) * (workerNo - 1));
        return (totalNoOfLines / 3) * (workerNo - 1);
    }

    private static int getWorkerPortNumber(int workerNo) {
        return Constants.BASE_WORKER_PORT + (workerNo - 1) * 20;
    }

}
