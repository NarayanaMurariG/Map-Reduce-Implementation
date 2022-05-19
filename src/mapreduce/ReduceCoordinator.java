package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.Callable;

public class ReduceCoordinator implements Callable<String> {

    private String intermediateFile;
    private UseCase useCase;
    private Map keyMapping;
    private int workerNo;

    public ReduceCoordinator(String intermediateFile, UseCase useCase,Map map,int workerNo) {
        this.intermediateFile = intermediateFile.trim();
        this.useCase = useCase;
        this.keyMapping = map;
        this.workerNo = workerNo;
    }

    @Override
    public String call() throws Exception {

        if(useCase.equals(UseCase.WORD_COUNT)){
            try {
                BufferedReader br = new BufferedReader(new FileReader(intermediateFile));
                String line = br.readLine();
                while (line != null) {

                    String[] words = line.split(",");
                    String key = words[0];
                    int value = Integer.parseInt(words[1]);

                    int worker = (Math.abs(key.hashCode())) % 3 + 1;
//                    System.out.println("Key : "+key+" HashCode : "+worker);
                    if (worker == workerNo) {
                        // KeyMapping.computeIfAbsent(key, 1);
                        if(keyMapping.containsKey(key)){
                            int val = (int) keyMapping.get(key);
                            keyMapping.put(key,val+1);
                        }else{
                            keyMapping.put(key,1);
                        }

//                        keyMapping.putIfAbsent(key, value);
//                        keyMapping.computeIfPresent(key, (k, v) -> (int) v + 1);
                    }

                    // read next line
                    line = br.readLine();
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "Done";
    }
}
