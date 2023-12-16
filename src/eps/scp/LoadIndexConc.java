package eps.scp;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class LoadIndexConc implements Runnable {

    private final int option;
    private String indexDirPath;
    private Map<Integer, String> Files;
    private Map<Location, String> globalIndexFilesLines;
    private Map<String, HashSet<Location>> Hash;
    private File file;


    //Constructor de la clase

    public LoadIndexConc(int option, String indexDirPath){

        this.option = option;
        this.indexDirPath = indexDirPath;
    
    }

    public LoadIndexConc(int option, File file){

        this.option = option;
        this.file = file;

    }


    //Setters

    public void setFilesIds(Map<Integer, String> Files){
        this.Files = Files;
    }

    public void setFilesLines(Map<Location, String> IndexFilesLines){
        this.globalIndexFilesLines = IndexFilesLines;
    }

    public void setHash( Map<String, HashSet<Location>> Hash){
        this.Hash = Hash;
    }

    //getters

    public Map<Integer, String> getFiles(){return this.Files;}

    public Map<Location, String> getFilesLines(){return this.globalIndexFilesLines;}

    public Map<String, HashSet<Location>> getHash(){
        return this.Hash;
    }



    @Override
    public void run(){

        InvertedIndex task = new InvertedIndex();

        if(option == 1){

            task.loadInvertedIndex1(this.file);
            setHash(task.getHash());

        }else if(option == 2){

            task.loadFilesIds(this.indexDirPath);
            setFilesIds(task.getFiles());

        }else if(option == 3){
            
            task.loadFilesLines(this.indexDirPath);
            setFilesLines(task.getIndexFilesLines());

        }else{
            System.out.println("This option does not exisist");
        }

    }
    
}
