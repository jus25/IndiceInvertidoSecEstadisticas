package eps.scp;
import eps.scp.InvertedIndex;
import eps.scp.Location;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class taskGetIndexForFile implements Runnable {

    private List<File> FilesList;
    private int FileId;
    public Map<String, HashSet<Location>> Hash;
    private Map<Integer,String> Files;
    private Map<Location, String> IndexFilesLines;
    
    
    //constructor

    public taskGetIndexForFile(List<File> FilesList, int fileId){
        
        this.FilesList = FilesList;
        this.FileId = fileId;
        
    }

    //Setters


    public void setHasMap(Map<String, HashSet <Location>> Hash){
        this.Hash = Hash;
    }
    public void setFiles( Map<Integer,String> Files){
        this.Files = Files;
    }

    public void setIndexFilesLines(Map<Location, String> IndexFilesLines){
        this.IndexFilesLines = IndexFilesLines;
    }

    //getters

    public  Map<String, HashSet<Location>> getHashMap(){
        return this.Hash;
    }

    public  Map<Integer,String> getFiles(){
        return this.Files;    
    }

    public Map<Location, String> getIndexFilesLines(){
        return this.IndexFilesLines;
    }
    


    @Override
    public void run(){
        InvertedIndex getIndex = new InvertedIndex();
        getIndex.setFileList(FilesList);
        getIndex.buidIndexFiles(FileId);
        setHasMap(getIndex.getHash());
        setFiles(getIndex.getFiles());
        setIndexFilesLines(getIndex.getIndexFilesLines());
       
        
    }
    
}
