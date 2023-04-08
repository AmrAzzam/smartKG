/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatahdtfragment.model;

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.rdfhdt.hdt.rdf.parsers.JenaNodeCreator;

/**
 *
 * @author azzam
 */
public class StreamLDF implements Callable {

    ExtendedIterator<Triple> triples = null;
    Process p = null;
    Triple triple;
    Stopwatch stopwatch;

    public StreamLDF(Triple triple, Stopwatch s) {
        //System.out.println("StreamLDF");
        this.triple = triple;
        this.stopwatch = s;
    }

    @Override
    public ExtendedIterator<Triple> call() throws Exception {
        
        System.out.println("calStart: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        String subject = triple.getSubject().toString().equals("ANY") ? "?s" : triple.getSubject().toString();
        String object = triple.getObject().toString().equals("ANY") ? "?o" : triple.getObject().toString();
        String t = subject + " <" + triple.getPredicate().toString() + "> " +  object;
        
        String query = "CONSTRUCT {" + t +"}\n" +
                        "WHERE { " + t  + " .}";
        
       // String query = "CONSTRUCT { "+ t +"}\n" +
                    //    "WHERE{ ?s ?p ?o . }";
       // System.out.println(query);
        String[] params = new String [4];

            params[0] = "node";
            params[1] = "/home/azzam/Desktop/TPF/ClientAndServer-JS/TPF-Client.js/Client.js-master/bin/ldf-client";
            params[2] = "http://quantum.ai.wu.ac.at:8080/watdiv";
            params[3] = query;
        //String[] Command ={"node", "/home/azzam/Desktop/TPF/ClientAndServer-JS/TPF-Client.js/Client.js-master/bin/ldf-client", "http://quantum.ai.wu.ac.at:8080/watdiv",query };
 
       
       //String cmd = "node /home/azzam/Desktop/TPF/ClientAndServer-JS/TPF-Client.js/Client.js-master/bin/ldf-client http://quantum.ai.wu.ac.at:8080/watdiv " + query;
        
      // String cmd = "/home/azzam/Desktop/test.sh";
       try {

           /* System.out.println("Command: " +  Command[0]);
              System.out.println("Command: " +  Command[1]);
                System.out.println("Command: " +  Command[2]);
                  System.out.println("Command: " +  Command[3]);*/
            p = Runtime.getRuntime().exec(params);
            /* BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;         
            while ((line = in.readLine()) != null) {
                System.out.println("Reading Lines");
                System.out.println(line);
            }*/
        } catch (IOException ex) {
            Logger.getLogger(StreamLDF.class.getName()).log(Level.SEVERE, null, ex);
        }
        // System.out.println("Before ===>>"+p.isAlive());
        triples = ExtendedTripleIteratorNodeLDF.create(p.getInputStream(), stopwatch);
        // int error = p.waitFor();
        // System.out.println("After ===>>"+p.isAlive());
        // System.out.println("Error: " + error);
        // System.out.println("Test: " + triples.next());
          
          System.out.println("callEnd: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));       
        return triples;
    }

}
