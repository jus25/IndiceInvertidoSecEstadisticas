package eps.scp;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryConc implements Runnable {


    Map<Location, Integer> queryMatchings;

    String word;

    ConcurrentHashMap<String, HashSet<Location>> globalLoadIndexInvertedMap;
   

    //constructor

    public QueryConc(String word, ConcurrentHashMap<String, HashSet<Location>> hash){

        this.word = word;
        this.globalLoadIndexInvertedMap = hash;
    

    }
    
    //setters

    private void setQueryMatching(Map<Location, Integer> queryMatching){

        this.queryMatchings = queryMatching;

    }

    //getters

    public Map<Location, Integer> getQueryMatching(){
        return this.queryMatchings;
    }



    @Override
    public void run(){
        InvertedIndex task = new InvertedIndex();
        task.setHash(globalLoadIndexInvertedMap);
        setQueryMatching(task.queryConc(word));       
        
    }





    
}
