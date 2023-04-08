/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatahdtfragment.model;

import QueryAnalyzer.Downloader.FamiliesHdtCache;
import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.linkeddatafragments.client.LinkedDataFragmentsClient;
import org.linkeddatafragments.model.ExtendedTripleIteratorLDF;
import org.linkeddatafragments.model.LinkedDataFragment;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;
import org.linkeddatafragments.solver.LDFStatistics;
import org.linkeddatafragments.solver.ReorderTransformationLDF;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;

/**
 *
 * @author azzam
 */
public class LinkedDataHdtFragmentGraph extends LinkedDataFragmentGraph {

    private HashMap<String, List<Integer>> queryStarsFamilies;
    private HashMap<TripleWrapper, String> queryTripleByStar;
    protected ReorderTransformation reorderTransform;
    protected LDFStatistics ldfStatistics;
    String dataSource;
    private final FamiliesHdtCache FamiliesCache;
    private ExtendedIterator<Triple> triples = null;
    int count = 0;
    //LinkedDataFragmentsClient ldfClient;
    int x = 0;
    int y = 0;
    int z = 0;
    //ProcessBuilder builder;
    // Process p = null;
    Stopwatch stopwatch = Stopwatch.createStarted();
    //ExecutorService pool = Executors.newCachedThreadPool();

    public LinkedDataHdtFragmentGraph(String dataSource) {
        super(dataSource);
        
        this.dataSource = dataSource;
        this.FamiliesCache = FamiliesHdtCache.getInstance();
        // System.out.println("======= Start of reorderTransform ======= ");
        this.reorderTransform = new ReorderTransformationLDF(this);
        //  System.out.println("======= End of reorderTransform ======= ");
        //
        //  System.out.println("======= Start of ldfStatistics ======= ");
        this.ldfStatistics = new LDFStatistics(this);  //must go after ldfClient created
        // System.out.println("======= End of ldfStatistics ======= ");

        //  ldfClient = new LinkedDataFragmentsClient(dataSource);
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        TripleWrapper tw = new TripleWrapper(triple.getSubject(), triple.getPredicate(), triple.getObject());
        String StarID = queryTripleByStar.get(tw);
       
        if (StarID != null && queryStarsFamilies.get(StarID) != null) {
            //  System.out.println("HDTTotalStart: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
            //   System.out.println("StarID: " + StarID);
            // System.out.println("hdtSolution");
            // System.out.println("Triple: " + triple);
            List<Integer> families = queryStarsFamilies.get(StarID);
            // System.out.println(families); 
            //    System.out.println("Famliess ==> "+ families.size());
            ArrayList<IteratorTripleString> hdtTotalResults = new ArrayList<>();
            
            for (Integer familyID : families) {
                try {

                    
                    HDT hdt = FamiliesCache.getEntry(familyID);
                    String subject = tw.getSubject().toString();
                    String object = tw.getObject().toString();
                    if (subject.equals("ANY")) {
                        subject = "";
                    }
                    if (object.equals("ANY")) {
                        object = "";
                    }
                    //   System.out.println("Subject ===>" + subject);
                    //  System.out.println("Predicate ===>" + tw.getPredicate().toString());
                    // System.out.println("Object ===>" + object);
                    IteratorTripleString its = hdt.search(subject, tw.getPredicate().toString(), object);
                    // System.out.println("====HDT Serach results======" + count ++);
                    //  System.out.println("iterate: " +    its.estimatedNumResults());

                    /*   while (its.hasNext()) {
                      
                        System.out.println(its.next());
                    }*/
                    if (its.hasNext()) {
                        hdtTotalResults.add(its);
                    }

                } catch (ExecutionException ex) {
                    Logger.getLogger(LinkedDataHdtFragmentGraph.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NotFoundException ex) {
                    Logger.getLogger(LinkedDataHdtFragmentGraph.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

           

            triples = ExtendedTripleIteratorHdt.create(hdtTotalResults);
            
            // System.out.println("HDTTotalEnd: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
            //  System.out.println(triples.next());
            return triples;
        } else {
            try {
                //   System.out.println("LDFTotalStart: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
                 //System.out.println("LDFSolution");
                // System.out.println("Triple: " + triple);
                /* if ("http://purl.org/stuff/rev#totalVotes".equals(triple.getPredicate().toString()))
                {
                    x++;
                     System.out.println("totalVotes" + x + " " + triple);
                }
                
                System.out.println(triple.getPredicate().toString());
                if ("http://purl.org/stuff/rev#hasReview".equals(triple.getPredicate().toString()))
                {
                    y++;
                    System.out.println("#hasReview" + y + " " + triple);
                }
                
                if ("http://schema.org/legalName".equals(triple.getPredicate().toString()))
                {
                    z++; 
                    System.out.println("#legalName" + z + " " + triple);
                }*/

                LinkedDataFragment ldf = ldfClient.getFragment(ldfClient.getBaseFragment(), triple);
                triples = ExtendedTripleIteratorLDF.create(ldfClient, ldf);

                return triples;
            } catch (Exception ex) {
                Logger.getLogger(LinkedDataHdtFragmentGraph.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return null;
    }

    /**
     *
     * @param queryTripleByStar
     */
    public void setQueryTripleByStar(HashMap<TripleWrapper, String> queryTripleByStar) {
       
        this.queryTripleByStar = queryTripleByStar;

    }

    public void setQueryStarsFamilies(HashMap<String, List<Integer>> queryStarsFamilies) {
        this.queryStarsFamilies = queryStarsFamilies;
        
    }

    public void closeIterators() {
        triples.close();
    }
    
    
    
    
    

}
