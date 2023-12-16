package eps.scp;

import java.io.File;
import java.util.List;

public class TaskGetDirectory implements Runnable {


    private InvertedIndex hash;
    private  List<File> FilesList;
    


    //Constructor
    public TaskGetDirectory(InvertedIndex hash){
        this.hash = hash;
        
    }

    //seters

    public void setFileList( List<File> FilesList){
        this.FilesList = FilesList;
    }

    //getters

    public List<File> getFilesList(){
        return this.FilesList;
    }

    @Override
    public void run(){

        hash.searchDirectoryFiles(hash.getInputDirPath());

        setFileList(hash.getFilesList());
          
    }
    
}
