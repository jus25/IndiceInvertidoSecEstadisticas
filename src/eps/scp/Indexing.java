
/* ---------------------------------------------------------------
Práctica 1.
Código fuente: gestor.c
Grau Informàtica
NIF i Nombre completo autor1. 54483858D  Miquel Elena Pérez
NIF i Nombre completo autor2. X6961232Y Abdellah Lamrabat
NIF i Nombre completo autor3. Y7426280P Samer Yousef Qaid , Saeed
--------------------------------------------------------------- */





package eps.scp;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Indexing {
    public static final boolean Verbose = false;
    private static final int DIndexMaxNumberOfFiles = 200; // Número máximo de ficheros para salvar el índice invertido.
    private static final int DIndexMinNumberOfFiles = 2; // Número mínimo de ficheros para salvar el índice invertido.
    private static final int DKeysByFileIndex = 1000;
    private static int numThreads;
    private static final String DIndexFilePrefix = "IndexFile";

    public static void main(String[] args) {
        InvertedIndex hash;

        if (args.length != 2)
            System.err.println("Erro in Parameters. Usage: Indexing <SourceDirectory> [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex(args[0]);
        else
            hash = new InvertedIndex(args[0], args[1]);

        Instant start = Instant.now();

        // Obtenció de la llista de fitxers per procesar directoris
        TaskGetDirectory task = new TaskGetDirectory(hash);
        searchDirectoryFile(task);
        List<File> FilesList = task.getFilesList();

        // S'obté el index invertit de cada fitxer
        Thread[] invertedIndexfile = new Thread[FilesList.size()];
        taskGetIndexForFile[] tasks = new taskGetIndexForFile[FilesList.size()];
        getForFileIndexInverted(task, invertedIndexfile, tasks);

        // Es junten tots els index invertits de cada fitxer en un sol index
        ConcurrentHashMap<String, HashSet<Location>> globalIndexInvertedMap = new ConcurrentHashMap<>();
        joinIndecesOnGlobalMap(globalIndexInvertedMap, invertedIndexfile, tasks);
        // printConcurrentHashMap(globalIndexInvertedMap);

        // Es reconstrueix unin tots els Files obtinguts en les execucions dels fils
        ConcurrentHashMap<Integer, String> GlobalFiles = new ConcurrentHashMap<>();
        joinFiles(tasks, GlobalFiles);

        // Es reconstrueix unin tots els FilesLines obtinguts en les execucions els fils
        TreeMap<Location, String> globalIndexFilesLines = new TreeMap<>();
        joinIndexFilesLines(tasks, globalIndexFilesLines);

        // Iniciem el proces del guardat del index invertit
        saveIndex(args[1], GlobalFiles, globalIndexFilesLines, globalIndexInvertedMap);

        // Iniciem el proces de carregat del index invertit

        ConcurrentHashMap<String, HashSet<Location>> globalLoadIndexInvertedMap = new ConcurrentHashMap<>();
        Map<Integer, String> globalLoadFiles = new ConcurrentHashMap<>();
        TreeMap<Location, String> globalLoadIndexFilesLines = new TreeMap<>();

        loadIndex(args[1], globalLoadIndexInvertedMap, globalLoadFiles, globalLoadIndexFilesLines);


        // Comprobar que el Indice Invertido cargado sea igual al salvado

        try {
            assertEquals(globalLoadFiles, GlobalFiles);
            assertEquals(globalIndexFilesLines, globalLoadIndexFilesLines);
            assertEquals(globalIndexInvertedMap, globalLoadIndexInvertedMap);
        } catch (AssertionError e) {
            System.out.println(hash.ANSI_RED + e.getMessage() + " " + hash.ANSI_RESET);
        }


        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis(); // in millis
        System.out.printf("[All Stages] Total execution time: %.3f secs.\n", timeElapsed / 1000.0);
    }

    public static void printConcurrentHashMap(ConcurrentHashMap<String, HashSet<Location>> globalIndexInvertedMap) {

        Set<String> keySet = globalIndexInvertedMap.keySet();
        for (String word : keySet) {
            System.out.print(word + "\t");
            HashSet<Location> locations = globalIndexInvertedMap.get(word);
            for (Location loc : locations) {
                System.out.printf("(%d,%d) ", loc.getFileId(), loc.getLine());
            }
            System.out.println();
        }

    }

    public static void searchDirectoryFile(TaskGetDirectory task) {

        Thread getDirectory = Thread.startVirtualThread(task);

        try {
            getDirectory.join();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Error en la obtenció de la llista de fitxers");
            e.printStackTrace();
        }

    }

    public static void getForFileIndexInverted(TaskGetDirectory task, Thread[] invertedIndexfile,
                                               taskGetIndexForFile[] tasks) {

        List<File> FilesList = task.getFilesList();

        int FileId = 0;

        for (File file : FilesList) {
            List<File> newFilesList = new ArrayList<>();
            newFilesList.add(file);
            taskGetIndexForFile taskFile = new taskGetIndexForFile(newFilesList, FileId);
            Thread thread = Thread.startVirtualThread(taskFile);

            invertedIndexfile[FileId] = thread;
            tasks[FileId] = taskFile;

            FileId++;
        }
    }

    public static void joinIndecesOnGlobalMap(ConcurrentHashMap<String, HashSet<Location>> globalIndexInvertedMap,
                                              Thread[] invertedIndexfile, taskGetIndexForFile[] tasks) {

        for (int i = 0; i < invertedIndexfile.length; i++) {
            try {
                invertedIndexfile[i].join();
                Map<String, HashSet<Location>> Hash = tasks[i].getHashMap();

                for (String keyString : Hash.keySet()) {

                    if (globalIndexInvertedMap.containsKey(keyString)) {
                        HashSet<Location> locations = globalIndexInvertedMap.get(keyString);
                        HashSet<Location> newLocation = Hash.get(keyString);
                        locations.addAll(newLocation);
                    } else {
                        globalIndexInvertedMap.put(keyString, Hash.get(keyString));
                    }

                }
            } catch (Exception e) {
                // TODO: handle exception
                System.out.println("Error en la obtenció de la llista de fitxers");
                e.printStackTrace();
            }

        }
    }

    public static void joinFiles(taskGetIndexForFile[] tasks, ConcurrentHashMap<Integer, String> GlobalFiles) {

        for (taskGetIndexForFile task : tasks) {

            Map<Integer, String> newMap = task.getFiles();

            for (Integer key : newMap.keySet()) {

                GlobalFiles.put(key, newMap.get(key));
            }
        }

    }

    public static void joinIndexFilesLines(taskGetIndexForFile[] tasks,
                                           TreeMap<Location, String> globalIndexFilesLines) {

        for (taskGetIndexForFile task : tasks) {

            Map<Location, String> IndexFilesLines = task.getIndexFilesLines();

            for (Location key : IndexFilesLines.keySet()) {

                globalIndexFilesLines.put(key, IndexFilesLines.get(key));

            }
        }

    }

    public static void saveIndex(String indexPath,
                                 ConcurrentHashMap<Integer, String> GlobalFiles,
                                 TreeMap<Location, String> globalIndexFilesLines,
                                 ConcurrentHashMap<String, HashSet<Location>> globalIndexInvertedMap) {

        // S'inicia el proces de netejar el directori especificat per l'usuari on es
        // guardarà el index invertit

        SaveIndexConc cleanDirectory = new SaveIndexConc(1, indexPath);
        Thread threadCleanDirectory = Thread.startVirtualThread(cleanDirectory);

        try {

            threadCleanDirectory.join();

        } catch (Exception e) {
            System.out.println("Error en la neteja del directori");
            e.printStackTrace();
        }

        // Aquesta fil s'encarrega de inciar el proces de crear l'arxiu que guardarà les
        // rutes dels fitxers procesats

        SaveIndexConc FilesIds = new SaveIndexConc(3, indexPath, globalIndexFilesLines, GlobalFiles,
                globalIndexInvertedMap);
        Thread threadFilesIds = Thread.startVirtualThread(FilesIds);

        // Aquest fil es la que s'encarrega de crear l'arxiu que contindrà les linies de
        // tots els fitxers indexades per numero y documents de tots els fitxers en un
        // sol arixu
        SaveIndexConc saveFilesLines = new SaveIndexConc(4, indexPath, globalIndexFilesLines, GlobalFiles,
                globalIndexInvertedMap);
        Thread threadSaveFilesLines = Thread.startVirtualThread(saveFilesLines);

        // Aquesta funció s'encarrega de guardar en un arxiu el index invertit
        saveInvertedIndex(indexPath, globalIndexInvertedMap);

        // esperem que acabi la execució dels fils
        try {
            threadFilesIds.join();
            threadSaveFilesLines.join();
        } catch (Exception e) {
            System.out.println("Error en el salvat del invertedIndex Op.1 Op.3 Op.4");
            e.printStackTrace();
        }
    }

    public static void saveInvertedIndex(String indexPath,
                                         ConcurrentHashMap<String, HashSet<Location>> globalIndexInvertedMap) {

        List<Map.Entry<String, HashSet<Location>>> newGlobalHash = hashToList(globalIndexInvertedMap);
        numThreads = getNumFiles(globalIndexInvertedMap);
        Thread[] threads = new Thread[numThreads];
        int remainingFiles, numFile = 0;
        long remainingKeys, keysByFile, end = 0, begin = 0;

        remainingKeys = newGlobalHash.size();
        remainingFiles = numThreads;

        for (int i = 0; i < numThreads; i++) {

            keysByFile = remainingKeys / remainingFiles;
            remainingKeys -= keysByFile;
            end += keysByFile;

            SaveIndexConc saveInvertedIndex = new SaveIndexConc(2, indexPath, newGlobalHash, begin, end, numFile);
            Thread threadSaveInvertedIndex = Thread.startVirtualThread(saveInvertedIndex);
            threads[i] = threadSaveInvertedIndex;

            begin = end;
            remainingFiles -= 1;
            numFile++;

        }

        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                System.out.println("Error en salvat del InvertedIndex Op.2");
                e.printStackTrace();
            }

        }

    }

    public static int getNumFiles(ConcurrentHashMap<String, HashSet<Location>> globalHash) {

        int numberOfFiles;
        // Charset utf8 = StandardCharsets.UTF_8;
        Set<String> keySet = globalHash.keySet();

        numberOfFiles = keySet.size() / DKeysByFileIndex;
        // Calculamos el número de ficheros a crear en función del número de claves que
        // hay en el hash.
        if (numberOfFiles > DIndexMaxNumberOfFiles)
            numberOfFiles = DIndexMaxNumberOfFiles;
        if (numberOfFiles < DIndexMinNumberOfFiles)
            numberOfFiles = DIndexMinNumberOfFiles;

        return numberOfFiles;
    }

    public static List<Map.Entry<String, HashSet<Location>>> hashToList(Map<String, HashSet<Location>> globalHash) {

        return new ArrayList<>(globalHash.entrySet());
    }

    public static void loadIndex(String indexDirPath,
                                 ConcurrentHashMap<String, HashSet<Location>> globalLoadIndexInvertedMap,
                                 Map<Integer, String> globalLoadFiles,
                                 Map<Location, String> globalLoadIndexFilesLines) {

        // Es carrega el index files id i el files lines a memoria

        LoadIndexConc loadFilesIds = new LoadIndexConc(2, indexDirPath);
        LoadIndexConc loadFilesLines = new LoadIndexConc(3, indexDirPath);

        Thread threadFilesIds = Thread.startVirtualThread(loadFilesIds);
        Thread threadFilesLines = Thread.startVirtualThread(loadFilesLines);

        loadInvertedIndex(globalLoadIndexInvertedMap, indexDirPath);

        try {
            threadFilesIds.join();
            threadFilesLines.join();

        } catch (Exception e) {
            System.out.println("Error en el salvat del invertedIndex Op.1 Op.3 Op.4");
            e.printStackTrace();
        }

        globalLoadFiles.putAll(loadFilesIds.getFiles());

        globalLoadIndexFilesLines.putAll(loadFilesLines.getFilesLines());


    }

    public static void loadInvertedIndex(ConcurrentHashMap<String, HashSet<Location>> globalLoadIndexInvertedMap,
                                         String indexDirPath) {

        File folder = new File(indexDirPath);
        File[] listOfFiles = folder.listFiles((d, name) -> name.startsWith(DIndexFilePrefix));
        Thread[] threads = new Thread[listOfFiles.length];
        LoadIndexConc[] loadInvertedIndexArray = new LoadIndexConc[listOfFiles.length];
        int i = 0;

        // Recorremos todos los ficheros del directorio de Indice y los procesamos.
        for (File file : listOfFiles) {
            if (file.isFile()) {

                LoadIndexConc loadInvertedIndex = new LoadIndexConc(1, file);
                Thread threadLoadInvertedIndex = Thread.startVirtualThread(loadInvertedIndex);
                threads[i] = threadLoadInvertedIndex;
                loadInvertedIndexArray[i] = loadInvertedIndex;
                i++;
            }
        }

        for (int j = 0; j < listOfFiles.length; j++) {
            try {
                threads[j].join();

            } catch (Exception e) {
                System.out.println("Error en el cargat del InvertedIndex Op.2");
                e.printStackTrace();
            }
        }

        for (i = 0; i < loadInvertedIndexArray.length; i++) {

            Map<String, HashSet<Location>> ParcialHash = loadInvertedIndexArray[i].getHash();

            for (String Key : ParcialHash.keySet()) {

                globalLoadIndexInvertedMap.put(Key, ParcialHash.get(Key));
            }

        }
    }
}