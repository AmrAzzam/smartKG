/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer.Downloader;

import Evaluation.Evaluation;
import QueryAnalyzer.FamiliesConfig;
import QueryAnalyzer.Family;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.linkeddatafragments.utils.Config;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

/**
 *
 * @author azzam
 */
public class FamiliesHdtCache {

    private static final FamiliesHdtCache familiesHdtCache = new FamiliesHdtCache();
    private final LoadingCache<Integer, HDT> cache;
    //Stopwatch stopwatch = Stopwatch.createStarted();

    private FamiliesHdtCache() {
       
        
        cache = CacheBuilder.newBuilder()
                .maximumSize(20000) //Maximum caching size
                .build(new CacheLoader<Integer, HDT>() {
                    @Override
                    public HDT load(Integer k) throws Exception {
                       // System.out.println("Load");
                        return addcache(k);
                    }

                });
        
        

    }

    public void releaseCache() {

       // System.out.println("Cache Size:" + cache.size());
        cache.asMap().entrySet().forEach((m) -> {
            try {
               // System.out.println("releaseCache");
                //Object key = m.getKey();
                m.getValue().close();
            } catch (IOException ex) {
                Logger.getLogger(FamiliesHdtCache.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        //System.out.println("Cache Size:" + cache.size());
        cache.invalidateAll();
       
        //familiesHdtCache = null;
        // familiesHdtCache = new FamiliesHdtCache();
    }

    public static FamiliesHdtCache getInstance() {

        return familiesHdtCache;
    }

    private HDT addcache(Integer family) {
        // System.out.print("adding chache");
        HDT hdt = null;
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            System.out.println("Start Caching " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

            Family f = FamiliesConfig.getInstance().getFamilyByID(family);

            //   System.out.println("Hdt File" + f.getName());
            String filePathString = Config.getInstance().getDownloadedpartitions() + f.getName();
            String fileindexPathString = Config.getInstance().getDownloadedpartitions() + f.getName() + ".index.v1-1";
            File hdtfile = new File(filePathString);
            File indexfile = new File(fileindexPathString);

            // We will download if the file is not downloaded before 
            //Or Reusing the cache is not enabled 
            if (!hdtfile.exists() || !Config.getInstance().isCacheResued()) {
                //    System.out.println("Start Download: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));  

                //FTPDownloader.DownloadFamily(f);
                HTTPDownloader.DownloadFamily(f);

                System.out.println("End Download: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }
            //System.out.println("indexing" + filePathString);
            Evaluation.estimatedNumberOfDownloadedPartitionsPerQuery++;
            Evaluation.estimatedNumberOfDownloadedPartitionsPerWorkload++;
            Evaluation.estimatedTotalSizeOfDownloadedPartitionsPerQuery += hdtfile.length() / (1024 * 1024);
            Evaluation.estimatedTotalSizeOfDownloadedPartitionsPerQuery += indexfile.length() / (1024 * 1024);
            Evaluation.estimatedTotalSizeOfDownloadedPartitionsPerWorkload += hdtfile.length() / (1024 * 1024);
            Evaluation.estimatedTotalSizeOfDownloadedPartitionsPerWorkload += indexfile.length() / (1024 * 1024);
            hdt = HDTManager.mapIndexedHDT(filePathString, null);
             
            
            System.out.println("End Caching " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        } catch (IOException ex) {
            Logger.getLogger(FamiliesHdtCache.class.getName()).log(Level.SEVERE, null, ex);
        }
        return hdt;
    }

    public HDT getEntry(Integer i) throws ExecutionException {
        //System.out.println("FamiliesHdtCache => " + cache.asMap().keySet().size() );
        
        return cache.get(i);

    }

}

/*

 try {
                        String s = null;
                        Process p;
                        String[] cmd = {"/bin/bash", "-c", "ps -e -T -f | grep -i java | wc"};
                        p = Runtime.getRuntime().exec(cmd);

                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                        // read the output from the command
                        //System.out.println("Here is the standard output of the command:\n");
                        while ((s = stdInput.readLine()) != null) {
                            System.out.println("After LinkedDataHDTFragment Process " + s);
                        }

                    } catch (IOException ex) {
                        Logger.getLogger(LinkedDataHdtFragmentGraph.class.getName()).log(Level.SEVERE, null, ex);
                    }

*/