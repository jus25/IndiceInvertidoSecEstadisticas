package eps.scp;

import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Nando on 8/10/19.
 */
public class Query
{
    

    public static void main(String[] args)
    {
        InvertedIndex hash = new InvertedIndex();
        
        String queryString=null, indexDirectory=null;

        if (args.length !=2)
            System.err.println("Error in Parameters. Usage: Query <String> <IndexDirectory>");
        if (args.length > 0)
            queryString = args[0];
        if (args.length > 1)
            indexDirectory = args[1];

        

        //Carregat del index invertit

        ConcurrentHashMap<String, HashSet<Location>> globalLoadIndexInvertedMap = new ConcurrentHashMap<>();
        Map<Integer, String> globalLoadFiles = new ConcurrentHashMap<>();
        TreeMap<Location, String> globalLoadIndexFilesLines = new TreeMap<>();

        Indexing.loadIndex(indexDirectory,globalLoadIndexInvertedMap,globalLoadFiles, globalLoadIndexFilesLines);


        //Creacion de los diferentes mapas Query Matching

        Instant start = Instant.now();

        System.out.println("Searching for query: " + queryString);

        // Pre-procesamiento query
        queryString = Normalizer.normalize(queryString, Normalizer.Form.NFD);
        queryString = queryString.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        String filter_line = queryString.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");
       
        // Dividimos la línea en palabras.
        String[] words = filter_line.split("\\W+");
        int querySize = words.length;

        QueryConc [] tasks = new QueryConc[querySize];
        Thread[] threads = new Thread[querySize];

        int i = 0;

        for (String word : words) {
            
            QueryConc task = new QueryConc(word,globalLoadIndexInvertedMap);
            tasks[i] = task;
            threads[i] = Thread.startVirtualThread(task);
            i++;
        }

    
        for (int index = 0; index < querySize; index++) {
            try{
            threads[index].join();
           
            }catch(InterruptedException e){
                e.printStackTrace();
            }                       
        }

        Map<Location, Integer> globalQueryMatching = new TreeMap<Location, Integer>();
        
        joinQueryMatchings(tasks, globalQueryMatching);

        InvertedIndex getStats = new InvertedIndex();
        getStats.setIndexFilesLines(globalLoadIndexFilesLines);
        getStats.stats(globalQueryMatching, querySize);

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Query with %d words] Total execution time: %.3f secs.\n", querySize, timeElapsed/1000.0);
    }


    public static void joinQueryMatchings(QueryConc[] tasks, Map<Location, Integer> globalQueryMatching){

        for (int index = 0; index < tasks.length; index++) {
        
            Map<Location, Integer> actualQueryMatching = tasks[index].getQueryMatching();

            for(Location loc: actualQueryMatching.keySet()){
                
                if (globalQueryMatching.containsKey(loc)) {
                    int value = actualQueryMatching.get(loc);
                    globalQueryMatching.put(loc, value + globalQueryMatching.get(loc));
                    
                } else {
                    globalQueryMatching.put(loc, actualQueryMatching.get(loc));
                }
            }   
            
        }

    }


}
