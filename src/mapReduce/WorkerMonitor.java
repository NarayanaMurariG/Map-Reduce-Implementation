package mapreduce;

public class WorkerMonitor implements Runnable {

    private int id;
    private int address;
    private ProcessType processType;

    public WorkerMonitor(int id, int address, ProcessType processType) {
        this.id = id;
        this.address = address;
        this.processType = processType;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    public ProcessType getProcessType() {
        return processType;
    }

    public void setProcessType(ProcessType processType) {
        this.processType = processType;
    }

    @Override
    public void run() {

    }
}
