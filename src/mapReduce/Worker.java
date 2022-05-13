package mapReduce;

import java.util.Locale;

public class Worker{

    private static ProcessType processType;
    private String filePath;
    private int startByte;
    private int endByte;

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
        return startByte;
    }

    public void setStartByte(int startByte) {
        this.startByte = startByte;
    }

    public int getEndByte() {
        return endByte;
    }

    public void setEndByte(int endByte) {
        this.endByte = endByte;
    }

    public static void main(String[] args) {
        if(args.length < 4){
            System.out.println("Invalid Number of Arguments");
            System.exit(0);
        }
        Worker worker = new Worker();
        try{
            if(args[0].toLowerCase() == "MAP"){
                worker.setProcessType(ProcessType.MAP);
            }else{
                worker.setProcessType(ProcessType.REDUCE);
            }
        }catch (Exception e){
            System.out.println("Exception Occured..! System.exit()");
            System.exit(0);
        }

        startWork();
    }

    private static void startWork() {

        hostServer();
        fetchInstructionsFromMaster();

        if(processType.equals(ProcessType.MAP)){
            startMapPhase();
        }else{
            startReducePhase();
        }

    }

    private static void startReducePhase() {

//        TODO Read from Intermediate file and compute final result, save to output file

    }

    private static void startMapPhase() {

//        TODO Read File Line By Line and split by space, write key,value pair to intermediate file

    }

    private static void hostServer() {
//        TODO Start Server
    }

    private static void fetchInstructionsFromMaster() {

//        TODO Wait for Master To Send Instructions

    }
}
