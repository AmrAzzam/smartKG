/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer.Downloader;

import QueryAnalyzer.FamiliesConfig;
import QueryAnalyzer.Family;
import com.google.common.base.Stopwatch;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author azzam
 */
public class FTPDownloader implements PartitionsDownloader {

    static FamiliesConfig familiesConfig = FamiliesConfig.getInstance();
    static String downloadFolder = "/home/azzam/Desktop/TPF/ClientAndServer-Java/Client.Java/downloadedPartitions/";
    //static Stopwatch stopwatch = Stopwatch.createStarted();
    static ChannelSftp sftpChannel; 
    
    JSch jsch = new JSch();
        Session session = null;

    public FTPDownloader() {

       
        try {
            session = jsch.getSession("amr", "quantum.ai.wu.ac.at", 22);

            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword("P@$$w0rd");
            session.connect();

            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftpChannel = (ChannelSftp) channel;
        } catch (JSchException ex) {
            Logger.getLogger(FTPDownloader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void DownloadPartitionsByStars(HashMap<String, List<Integer>> querySolutionsPartitions) {

        try {
            // System.out.println("querySolutionsPartitions" + querySolutionsPartitions);
            /*// APPROACH #1: using retrieveFile(String, OutputStream)*/
            for (Map.Entry<String, List<Integer>> entry : querySolutionsPartitions.entrySet()) {
                String key = entry.getKey();
                List<Integer> familiesIds = entry.getValue();

                /* if it is only one predicate then we will process it using LDF
                if (familiesIds != null && familiesIds.size() == 1 && !(FamiliesConfig.getInstance().getFamilyByID(familiesIds.get(0)).isGrouped()) ) {
                    
                  //  System.out.println("NoDownload");
                    continue;
                }*/

                for (Integer familyId : familiesIds) {

                    // we will not download the family if it already exists
                    if (FamiliesHdtCache.getInstance().getEntry(familyId) == null) {
                       
                          //System.out.println("Download");
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
        String sourceHdt = "/home/fernandez/tpf-molecule/hdt-cpp-molecules/libhdt/data/part_families_watdiv_10M/hdt/" + family.getName();
        String destinationFile = downloadFolder.concat(family.getName());
       // System.out.println(sourceHdt);
        try {
            sftpChannel.get(sourceHdt, destinationFile);

            String sourceIndex = "/home/fernandez/tpf-molecule/hdt-cpp-molecules/libhdt/data/part_families_watdiv_10M/hdt/" + family.getName() + ".index.v1-1";

            String destinationIndexFile = downloadFolder.concat(family.getName() + ".index.v1-1");

            //  System.out.println("End Download "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
            sftpChannel.get(sourceIndex, destinationIndexFile);
        } catch (SftpException ex) {
            Logger.getLogger(FTPDownloader.class.getName()).log(Level.SEVERE, null, ex);
        }
       

    }

    @Override
    public void closeConnection() {

        session.disconnect();
        sftpChannel.exit();
    }

}


/*
 // APPROACH #2: using InputStream retrieveFileStream(String)
 
            String remoteFile2 = "/test/song.mp3";
             
            File downloadFile2 = new File("D:/Downloads/song.mp3");
             
            OutputStream outputStream2 = new BufferedOutputStream(new FileOutputStream(downloadFile2));
             
            InputStream inputStream = ftpClient.retrieveFileStream(remoteFile2);
             
            byte[] bytesArray = new byte[4096];
             
            int bytesRead = -1;
             
            while ((bytesRead = inputStream.read(bytesArray)) != -1) {
                  
                outputStream2.write(bytesArray, 0, bytesRead);
                 
            }
          success = ftpClient.completePendingCommand();
             
            if (success) {
                  
                System.out.println("File #2 has been downloaded successfully.");
                 
            }
             
           
             
            inputStream.close();
            outputStream2.close();
 */
