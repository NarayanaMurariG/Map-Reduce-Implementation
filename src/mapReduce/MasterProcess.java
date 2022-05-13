package mapReduce;

public class MasterProcess {

    private static String fileName;
    private static UseCase useCase;
    private static Setup setup;

    public static void main(String[] args){
        fileName = args[0];

        if(args[1].equalsIgnoreCase(UseCase.WORD_COUNT.toString())){
            useCase = UseCase.WORD_COUNT;
        }else{
            useCase = UseCase.REVERSE_WEB_LINK;
        }

        initaliseSetup();
        startMapPhase(useCase);
    }

    private static void initaliseSetup() {
        setup = Setup.getSetup();
    }

    private static void startMapPhase(UseCase useCase) {
        long fileSize = Utils.getFileSize(fileName);
        System.out.println("File Size is : "+fileSize);
        long chunk_size = fileSize / 3;
        System.out.println(Utils.getFileDataInRange(0,chunk_size-1,fileName));
        System.out.println(Utils.getFileDataInRange(chunk_size-1,2*chunk_size-1,fileName));
    }

}
