package eps.scp;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class SaveIndexConc implements Runnable {

    private final int option;
    private int numFile;
    private final String IndexDirPath;
    private ConcurrentHashMap<Integer, String> Files;
    private TreeMap<Location, String> globalIndexFilesLines;
    private ConcurrentHashMap<String, HashSet<Location>> globalHash;
    private List<Map.Entry<String, HashSet<Location>>> listHash;
    private long begin;
    private long end;
    CountDownLatch latch;

    //Constructors de la clase

    public SaveIndexConc(int options, String indexDirectory) {
        this.option = options;
        this.IndexDirPath = indexDirectory;
    }

    public SaveIndexConc(int options, String indexDirectory, TreeMap<Location, String> indexFilesLines, ConcurrentHashMap<Integer, String> Files, ConcurrentHashMap<String, HashSet<Location>> Hash) {
        this.option = options;
        this.IndexDirPath = indexDirectory;
        this.globalIndexFilesLines = indexFilesLines;
        this.Files = Files;
        this.globalHash = Hash;

    }

    public SaveIndexConc(int options, String indexDirectory, List<Map.Entry<String, HashSet<Location>>> listHash, long begin, long end, int numFile) {

        this.option = options;
        this.IndexDirPath = indexDirectory;
        this.listHash = listHash;
        this.begin = begin;
        this.end = end;
        this.numFile = numFile;

    }

    @Override
    public void run() {

        InvertedIndex task = new InvertedIndex();

        //Netejar el directori del output

        if (this.option == 1) {

            task.resetDirectory(IndexDirPath);

            //Aquesta funcio es la que guarda el index invertit

        } else if (this.option == 2) {
            task.saveInvertedIndex1(IndexDirPath, listHash, begin, end, numFile);

            //Aquesta funció es la que guarda els paths dels fitxers

        } else if (this.option == 3) {

            task.setFiles(this.Files);
            task.saveFilesIds(IndexDirPath);

            //Agrupa totes les linies de tots els arxius

        } else if (this.option == 4) {
            task.setIndexFilesLines(this.globalIndexFilesLines);
            task.saveFilesLines(IndexDirPath);

        } else {
            System.out.println("This option is not correct");
        }
        if (latch != null) {
            this.latch.countDown();
        }
    }


    public void setLatch(CountDownLatch saveInvertedIndexLatch) {
        this.latch=saveInvertedIndexLatch;
    }
}