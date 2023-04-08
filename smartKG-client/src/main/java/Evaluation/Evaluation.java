package Evaluation;

import com.google.common.base.Stopwatch;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.linkeddatafragments.utils.Config;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author azzam
 */
public class Evaluation {

    // Auto Increment
    private static int queryID;
    // Query File Name
    private static String queryName;

    // Execution time to Extract the Stars and the families  Done:D
    private static long queryAnlysisTimePerQuery = 0;

    // Number Of Downloaded Files Per Queries based on the Analyzer Done:D
    public static int estimatedNumberOfDownloadedPartitionsPerQuery = 0;

    // number Of Downloaded Files Per Queries based on the Analyzer Done:D
    public static int actualNumberOfDownloadedPartitionsPerQuery = 0;

    // Total Size of the downloaded File Per Query Done:D
    public static long estimatedTotalSizeOfDownloadedPartitionsPerQuery = 0;

    // Total Size of the downloaded File Per Query Done:D
    public static long actualTotalSizeOfDownloadedPartitionsPerQuery = 0;

    // Total Download Time for the files for each query Done:D
    private static long totalDownloadTimeOfPartitionsPerQuery = 0;
    
    public static int resultCountPerQuery = 0;

    // TotalExecutionTimePerQuery  Done:D
    private static long totalExecTimePerQuery = 0;

    // number Of estimated Downloaded  Files Per WorkLoad Done:D
    public static int estimatedNumberOfDownloadedPartitionsPerWorkload = 0;

    // number Of Downloaded Files Per WorkLoad Done:D
    public static int actualNumberOfDownloadedPartitionsPerWorkload = 0;

    // Total Size of the downloaded File Per Query Done:D
    public static long estimatedTotalSizeOfDownloadedPartitionsPerWorkload = 0;

    // Total Size of the downloaded File Per Query Done:D
    public static long actualTotalSizeOfDownloadedPartitionsPerWorkload = 0;

    // Total Download Time for the files for each WorkLoad
    private static long totalDownloadTimeOfPartitionsPerWorkLoad = 0;

    // TotalExecutionTimePerWorkLoad Done:D
    private static long totalExecTimePerWorkLoad = 0;
    // TotalExecutionTimePerWorkLoad Done:D
    private static long recieveFirstAnswerTimePerQuery = 0;

    // StopWatch Timer reset after each query
    private static Stopwatch queryStopWatch;

    // StopWatch Timer reset for the workLoad
    private static Stopwatch workloadStopWatch;
    
      

    private static final String SAMPLE_CSV_FILE = "./"+ Config.getInstance().getExperimentName() + ".csv";
    private static CSVPrinter TimeoutCsvPrinter;
    private static CSVPrinter csvPrinter;
    private static BufferedWriter writer;
    private static BufferedWriter TimeoutsBufferWriter;
    private static final String Timeout_FILE = "./" + Config.getInstance().getExperimentName() + "TimeOuts.csv";
    public static void queryTimeOutPrintCsv() {
        
        try {
            TimeoutCsvPrinter.printRecord(queryID,queryName);
        } catch (IOException ex) {
            Logger.getLogger(Evaluation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    

    

    public Evaluation (){
        
        try {
            writer = Files.newBufferedWriter(Paths.get(SAMPLE_CSV_FILE));
            TimeoutsBufferWriter = Files.newBufferedWriter(Paths.get(Timeout_FILE));
            
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader("queryID",
                            "queryName",
                            "queryAnlysisTimePerQuery",
                            "estimatedNumberOfDownloadedPartitionsPerQuery",
                            "actualNumberOfDownloadedPartitionsPerQuery",
                            "estimatedTotalSizeOfDownloadedPartitionsPerQuery",
                            "actualTotalSizeOfDownloadedPartitionsPerQuery",
                            "totalDownloadTimeOfPartitionsPerQuery",
                            "recieveFirstAnswerTimePerQuery",
                            "totalExecTimePerQuery",
                            "resultCountPerQuery"));
            
            TimeoutCsvPrinter  = new CSVPrinter(TimeoutsBufferWriter, CSVFormat.DEFAULT
                    .withHeader("queryID",
                            "queryName"));
        } catch (IOException ex) {
            Logger.getLogger(Evaluation.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    /**
     * @param aQueryID the queryID to set
     */
    public static void setQueryID(int aQueryID) {
        queryID = aQueryID;
    }

    /**
     * @param aQueryName the queryName to set
     */
    public static void setQueryName(String aQueryName) {
        queryName = aQueryName;
    }

    /**
     * void
     */
    public static void measureQueryAnlysisTimePerQuery() {
        queryAnlysisTimePerQuery = queryStopWatch.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     * @return the actualNumberOfDownloadedPartitionsPerQuery
     */
    public static int getActualNumberOfDownloadedPartitionsPerQuery() {
        return actualNumberOfDownloadedPartitionsPerQuery;
    }

    /**
     * @param ActualNumberOfDownloadedPartitionsPerQuery the
     * actualNumberOfDownloadedPartitionsPerQuery to set
     */
    public static void setActualNumberOfDownloadedPartitionsPerQuery(int ActualNumberOfDownloadedPartitionsPerQuery) {
        actualNumberOfDownloadedPartitionsPerQuery = ActualNumberOfDownloadedPartitionsPerQuery;
    }

    /**
     */
    public static void measureTotalDownloadTimeOfPartitionsPerQuery() {
        totalDownloadTimeOfPartitionsPerQuery = queryStopWatch.elapsed(TimeUnit.MILLISECONDS) - Evaluation.queryAnlysisTimePerQuery;
        totalDownloadTimeOfPartitionsPerWorkLoad += totalDownloadTimeOfPartitionsPerQuery;
    }

    /**
     */
    public static void measureTotalExecTimePerQuery() {
        totalExecTimePerQuery = queryStopWatch.elapsed(TimeUnit.MILLISECONDS);
    }
    
    public static void measureRecieveFirstAnswerTimePerQuery() {
        recieveFirstAnswerTimePerQuery = queryStopWatch.elapsed(TimeUnit.MILLISECONDS);

    }

    /**
     * @return the totalDownloadTimeOfPartitionsPerWorkLoad
     */
    public static long getTotalDownloadTimeOfPartitionsPerWorkLoad() {
        return totalDownloadTimeOfPartitionsPerWorkLoad;
    }

    /**
     * @param TotalDownloadTimeOfPartitionsPerWorkLoad the
     * totalDownloadTimeOfPartitionsPerWorkLoad to set
     */
    public static void setTotalDownloadTimeOfPartitionsPerWorkLoad(long TotalDownloadTimeOfPartitionsPerWorkLoad) {
        totalDownloadTimeOfPartitionsPerWorkLoad = TotalDownloadTimeOfPartitionsPerWorkLoad;
    }

    /**
     */
    public static void measureTotalExecTimePerWorkload() {
        totalExecTimePerWorkLoad = workloadStopWatch.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     */
    public static void startQueryTimer() {
        queryStopWatch = Stopwatch.createStarted();
    }

    /**
     *
     */
    public static void stopQueryTimer() {
        queryStopWatch.stop();
    }

    /**
     */
    public static void startWorkloadTimer() {
        workloadStopWatch = Stopwatch.createStarted();
    }

    /**
     *
     */
    public static void stopWorkloadTimer() {
        workloadStopWatch.stop();
    }

    
    public static void queryEvaluationToCsvFile() {

               
        try {
            
           
            csvPrinter.printRecord(queryID,
                    queryName,
                    queryAnlysisTimePerQuery,
                    estimatedNumberOfDownloadedPartitionsPerQuery,
                    actualNumberOfDownloadedPartitionsPerQuery,
                    estimatedTotalSizeOfDownloadedPartitionsPerQuery,
                    actualTotalSizeOfDownloadedPartitionsPerQuery,
                    totalDownloadTimeOfPartitionsPerQuery,
                    recieveFirstAnswerTimePerQuery,
                    totalExecTimePerQuery,
                    resultCountPerQuery);

        } catch (IOException ex) {
            Logger.getLogger(Evaluation.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void workLoadEvaluationToCsvFile() {

        try {
            csvPrinter.printRecord("");
            csvPrinter.printRecord("");
            csvPrinter.printRecord("");
            csvPrinter.printRecord(
                    "estimatedNumberOfDownloadedPartitionsPerWorkload",
                    "actualNumberOfDownloadedPartitionsPerWorkload",
                    "estimatedTotalSizeOfDownloadedPartitionsPerWorkload",
                    "actualTotalSizeOfDownloadedPartitionsPerWorkload",
                    "totalDownloadTimeOfPartitionsPerWorkLoad",
                    "totalExecTimePerWorkLoad");
            
            csvPrinter.printRecord(
                    estimatedNumberOfDownloadedPartitionsPerWorkload,
                    actualNumberOfDownloadedPartitionsPerWorkload,
                    estimatedTotalSizeOfDownloadedPartitionsPerWorkload,
                    actualTotalSizeOfDownloadedPartitionsPerWorkload,
                    totalDownloadTimeOfPartitionsPerWorkLoad,
                    totalExecTimePerWorkLoad);
            
            csvPrinter.flush();
            TimeoutCsvPrinter.flush();
        } catch (IOException ex) {
            Logger.getLogger(Evaluation.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }

    public static String queryEvaluationPrint() {

        String toString = "queryID"
                + "," + "queryName"
                + "," + "queryAnlysisTimePerQuery"
                + "," + "estimatedNumberOfDownloadedPartitionsPerQuery"
                + "," + "actualNumberOfDownloadedPartitionsPerQuery"
                + "," + "estimatedTotalSizeOfDownloadedPartitionsPerQuery"
                + "," + "actualTotalSizeOfDownloadedPartitionsPerQuery"
                + "," + "totalDownloadTimeOfPartitionsPerQuery"
                + "," + "recieveFirstAnswerTimePerQuery"
                + "," + "totalExecTimePerQuery" 
                + "," + "resultCountPerQuery"+ "\n";
        toString += queryID
                + "," + queryName
                + "," + queryAnlysisTimePerQuery
                + "," + estimatedNumberOfDownloadedPartitionsPerQuery
                + "," + actualNumberOfDownloadedPartitionsPerQuery
                + "," + estimatedTotalSizeOfDownloadedPartitionsPerQuery
                + "," + actualTotalSizeOfDownloadedPartitionsPerQuery
                + "," + totalDownloadTimeOfPartitionsPerQuery
                + "," + recieveFirstAnswerTimePerQuery
                + "," + totalExecTimePerQuery 
                + "," + resultCountPerQuery + "\n";
        return toString;
    }

    public static String workloadEvaluationPrint() {

        String toString = "estimatedNumberOfDownloadedPartitionsPerWorkload"
                + "," + "actualNumberOfDownloadedPartitionsPerWorkload"
                + "," + "estimatedTotalSizeOfDownloadedPartitionsPerWorkload"
                + "," + "actualTotalSizeOfDownloadedPartitionsPerWorkload"
                + "," + "totalDownloadTimeOfPartitionsPerWorkLoad"
                + "," + "totalExecTimePerWorkLoad" + "\n";

        toString += estimatedNumberOfDownloadedPartitionsPerWorkload
                + "," + actualNumberOfDownloadedPartitionsPerWorkload
                + "," + estimatedTotalSizeOfDownloadedPartitionsPerWorkload
                + "," + actualTotalSizeOfDownloadedPartitionsPerWorkload
                + "," + totalDownloadTimeOfPartitionsPerWorkLoad
                + "," + totalExecTimePerWorkLoad + "\n";
        return toString;
    }
}
