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

        //Hosting server and waiting for clients
        startServerAndListen();
    }


    /*
        Reduce phase takes intermediate files as input and also use case and uses worker coordinator threads to
        communicate with workers by sending intermediate files
        It gets the final output files from the workers which it returns to the client
    */
    private static List<String> startReducePhase(UseCase useCase,List<String> intermediateFiles) throws ExecutionException, InterruptedException {

        ExecutorService executors = Executors.newFixedThreadPool(Constants.MASTER_THREAD_POOL_SIZE);

        FutureTask<String>[] futureTasks = new FutureTask[Constants.SLAVE_COUNT];
        int workerPort;
        for(int i=1;i<=Constants.SLAVE_COUNT;i++){
            workerPort = getWorkerPortNumber(i);
            futureTasks[i-1] = new FutureTask(new WorkerCoordinator(workerPort,intermediateFiles.toString(),ProcessType.REDUCE,useCase));
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

            // Hosting server
            server = new ServerSocket(Constants.MASTER_PORT);
            while(true){

                // Waiting on incoming connections
                Socket socket = server.accept();

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                String inputFile = dis.readUTF();
                dos.writeUTF(Constants.OK);

                String useCaseStr = dis.readUTF();
                dos.writeUTF(Constants.OK);

                String pattern = null;
                if(useCaseStr.equalsIgnoreCase(UseCase.DISTRIBUTED_GREP.toString())){
                    pattern = dis.readUTF();
                    dos.writeUTF(Constants.OK);
                }

                System.out.println(dis.readUTF());

                fileName = inputFile;
                System.out.println("Use Case : " + useCaseStr);
                if(useCaseStr.equalsIgnoreCase(UseCase.WORD_COUNT.toString())){
                    useCase = UseCase.WORD_COUNT;
                }else if (useCaseStr.equalsIgnoreCase(UseCase.REVERSE_WEB_LINK.toString())){
                    useCase = UseCase.REVERSE_WEB_LINK;
                }else {
                    useCase = UseCase.DISTRIBUTED_GREP;
                }


                // Starting Map phase
                List<String> intermediateFiles = startMapPhase(useCase,pattern);
                System.out.println(intermediateFiles.toString());

                // Starting Reduce Phase
                List<String> outputFiles = startReducePhase(useCase,intermediateFiles);
                System.out.println("The final generated output files are : "+outputFiles.toString());


                String finalFile = "finalFile";
                dos.writeUTF(outputFiles.toString());

                System.out.println(dis.readUTF());

                dis.close();
                dos.close();
                socket.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /*
        Map phase takes input files, useCase and the distrubutes set of lines to each of the worker node using
        Worker Coordinator and then gets the final intermediate file as output from worker coordinator
     */
    private static List<String> startMapPhase(UseCase useCase, String pattern) throws Exception{

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
            System.out.println("Debug : "+ pattern);
            WorkerCoordinator coordinator = new WorkerCoordinator(workerPort,startLine,endLine,fileName,ProcessType.MAP,useCase);
            coordinator.setInputParameter(pattern);
            futureTasks[i-1] = new FutureTask(coordinator);
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
