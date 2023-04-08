/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.client;

import Evaluation.Evaluation;
import QueryAnalyzer.Downloader.FamiliesHdtCache;
import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.linkeddatafragments.utils.Config;
import org.linkeddatahdtfragment.model.LinkedDataHdtFragmentGraph;

/**
 *
 * @author azzam
 */
public class ExperimentStarter {

    private final Model model;
    private final Query query;

    ExperimentStarter(Model model, Query query) {
        this.model = model;
        this.query = query;

    }

    /**
     * Write the results of the query to the stream.
     *
     * @param outputStream The output stream.
     */
    public void writeResults(final PrintStream outputStream) {

        
          // System.out.println("After QueryExecution results :)");
        try ( //System.out.println("Before QueryExecution results :)");
                
                QueryExecution executor = QueryExecutionFactory.create(query, model)) {
            
             
            // System.out.println("After QueryExecution results :)");
            switch (query.getQueryType()) {
                case Query.QueryTypeSelect:
                    //       System.out.println("Before executor :)");
                    
                    final ResultSet rs = executor.execSelect();
                   
                    Evaluation.measureRecieveFirstAnswerTimePerQuery();
                    //int count = 0;
                    Stopwatch timeOut = Stopwatch.createStarted();
                    int noOfHeavyQueries = 0;
                    //executor.setTimeout(Config.getInstance().getTimeInMin(), TimeUnit.MINUTES);
                    System.out.println("Result => " + rs.hasNext());
                   while (rs.hasNext()) {
                       
                        System.out.println(rs.next().toString());
                    //}
                    
                   
                    
                    if(timeOut.elapsed(TimeUnit.MINUTES) >= Config.getInstance().getTimeInMin() ) {
                    System.out.println("Join timeOut:");
                    noOfHeavyQueries++;
                    Evaluation.queryTimeOutPrintCsv();
                    // System.out.println("Query =>" + timeOut.elapsed(TimeUnit.MINUTES) );
                    break;
                    }
                    
                    
                    
                    Evaluation.resultCountPerQuery++;
                    // System.out.println("Before one query solution :)" );
                   // System.out.println(rs.next().toString());
                    // outputStream.println();
                    //   System.out.println("After one query solution :)" + rs.getRowNumber());
                    //System.out.println("After one query solution :)" + rs.getRowNumber()); 
                    //  System.out.println("count" + count);
                    }
                   System.out.println("Sucess:");
                    FamiliesHdtCache.getInstance().releaseCache();
                    executor.close();
                    
                    //System.out.println("NoOfHeavyQueries" + noOfHeavyQueries);
                    timeOut.stop();
                    
                    break;
                    case Query.QueryTypeAsk:
                    outputStream.println(executor.execAsk());
                    break;
                    case Query.QueryTypeConstruct:
                    case Query.QueryTypeDescribe:
                    final Iterator<Triple> triples = executor.execConstructTriples();
                    while (triples.hasNext()) {
                    outputStream.println(triples.next());
                    }
                    break;
                    default:
                    throw new Error("Unsupported query type");
            }   
            //FamiliesHdtCache.getInstance().releaseCache();
            //  System.out.println("End of the wrting");
        }
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