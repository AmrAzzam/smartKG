/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer.Downloader;

import Evaluation.Evaluation;
import static QueryAnalyzer.Downloader.FTPDownloader.DownloadFamily;
import static QueryAnalyzer.Downloader.FTPDownloader.downloadFolder;
import QueryAnalyzer.FamiliesConfig;
import QueryAnalyzer.Family;
import QueryAnalyzer.ServerInteractionHandler;
import com.google.common.base.Stopwatch;
import com.jcraft.jsch.SftpException;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.linkeddatafragments.utils.Config;

/**
 *
 * @author azzam
 */
public class HTTPDownloader implements PartitionsDownloader {

    private final static FamiliesConfig familiesConfig = FamiliesConfig.getInstance();
    private final static String downloadFolder = Config.getInstance().getDownloadedpartitions();
    
     
    @Override
    public void DownloadPartitionsByStars(HashMap<String, List<Integer>> querySolutionsPartitions) {
        
        try {
                Stopwatch timeOut = Stopwatch.createStarted();
            // System.out.println("querySolutionsPartitions" + querySolutionsPartitions);
            /*// APPROACH #1: using retrieveFile(String, OutputStream)*/
            
            for (Map.Entry<String, List<Integer>> entry : querySolutionsPartitions.entrySet()) {
                String key = entry.getKey();
                List<Integer> familiesIds = entry.getValue();
                    if (timeOut.elapsed(TimeUnit.MINUTES) >= 10){
                        System.out.println("Download TimeOut");
                        Evaluation.queryTimeOutPrintCsv();
                        break;
                    }
                /* if it is only one predicate then we will process it using LDF
                if (familiesIds != null && familiesIds.size() == 1 && !(FamiliesConfig.getInstance().getFamilyByID(familiesIds.get(0)).isGrouped()) ) {
                    
                  //  System.out.println("NoDownload");
                    continue;
Time elapsed for Download is 0
Exception in thread "main" org.apache.jena.atlas.web.HttpException: org.apache.http.conn.HttpHostConnectExce
                }*/
                
                
                for (Integer familyId : familiesIds) {
                   //System.out.println("FamilyCache: " + FamiliesHdtCache.getInstance().getEntry(familyId));
             
                    // we will not download the family if it already exists
                    if (FamiliesHdtCache.getInstance().getEntry(familyId) == null) {
                      // System.out.println("hii");
                                            
                        System.out.println("The download is called fro");
                        Family f = FamiliesConfig.getInstance().getFamilyByID(familyId);
                        DownloadFamily(f);
                    }

                }

            }
            // System.out.println("File #1 has been downloaded successfully.");

        } catch (ExecutionException ex) {
            Logger.getLogger(FTPDownloader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void DownloadFamily(Family family) {

        
        //  System.out.println("Family==>" + familyId + "==" + f.getName());
        // System.out.println("Start Download "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        String sourceHdt = family.getName();
        String destinationFile = downloadFolder.concat(family.getName());
        String sourceIndex = family.getName() + ".index.v1-1";
        String destinationIndexFile = downloadFolder.concat(family.getName() + ".index.v1-1");
        // System.out.println(sourceHdt);
        // try {

        ServerInteractionHandler sih = new ServerInteractionHandler();
        sih.getServerFamilyData(sourceHdt, destinationFile);
        sih.getServerFamilyData(sourceIndex, destinationIndexFile);

        File hdtDest = new File(destinationFile);
        File indexDest = new File(destinationIndexFile);
        //System.out.println("xxx" +  Evaluation.actualNumberOfDownloadedPartitionsPerQuery);
        Evaluation.actualNumberOfDownloadedPartitionsPerQuery++;
        Evaluation.actualNumberOfDownloadedPartitionsPerWorkload++;
        
        Evaluation.actualTotalSizeOfDownloadedPartitionsPerQuery += hdtDest.length();
        Evaluation.actualTotalSizeOfDownloadedPartitionsPerQuery += indexDest.length();
        Evaluation.actualTotalSizeOfDownloadedPartitionsPerWorkload += hdtDest.length();
        Evaluation.actualTotalSizeOfDownloadedPartitionsPerWorkload += indexDest.length();
        
        
        // sftpChannel.get(sourceHdt, destinationFile);
        //  System.out.println("End Download "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        //   sftpChannel.get(sourceIndex, destinationIndexFile);
        // } catch (SftpException ex) {
        //   Logger.getLogger(FTPDownloader.class.getName()).log(Level.SEVERE, null, ex);
        //}
    }

    @Override
    public void closeConnection() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
