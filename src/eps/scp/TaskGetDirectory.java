package eps.scp;

import java.io.File;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

public class TaskGetDirectory implements Runnable {

    CyclicBarrier barrierStart;
    CyclicBarrier barrierEnd;
    private InvertedIndex hash;
    private List<File> FilesList;


    //Constructor
    public TaskGetDirectory(InvertedIndex hash, CyclicBarrier[] barriers) {
        this.hash = hash;
        this.barrierStart = barriers[0];
        this.barrierEnd = barriers[1];

    }

    //seters

    public void setFileList(List<File> FilesList) {
        this.FilesList = FilesList;
    }

    //getters

    public List<File> getFilesList() {
        return this.FilesList;
    }

    @Override
    public void run() {
        try {

            barrierStart.await();
            hash.searchDirectoryFiles(hash.getInputDirPath());
            setFileList(hash.getFilesList());
            barrierEnd.await();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
