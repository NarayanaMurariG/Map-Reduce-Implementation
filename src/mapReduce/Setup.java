package mapreduce;

import java.util.ArrayList;
import java.util.List;

public class Setup {

    private static Setup setup;
    private static List<String> workerMachines = new ArrayList<>();

    public static Setup getSetup() {
        if (setup == null) {
            setup = new Setup();
        }
        return setup;
    }

    public static List<String> getWorkerMachines() {
        return workerMachines;
    }

    public static void addMachine(String address) {
        workerMachines.add(address);
    }

}
